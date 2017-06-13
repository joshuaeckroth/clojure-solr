(ns clojure-solr
  (:require [clojure.string :as str])
  (:require [clj-time.core :as t])
  (:require [clj-time.format :as tformat])
  (:require [clj-time.coerce :as tcoerce])
  (:import (java.net URI)
           (java.util Base64 HashMap)
           (java.nio.charset Charset)
           (org.apache.http HttpRequest HttpRequestInterceptor HttpHeaders)
           (org.apache.http.auth AuthScope UsernamePasswordCredentials)
           (org.apache.http.client.protocol HttpClientContext)
           (org.apache.http.impl.auth BasicScheme)
           (org.apache.http.impl.client BasicCredentialsProvider HttpClientBuilder)
           (org.apache.http.protocol HttpContext HttpCoreContext)
           (org.apache.solr.client.solrj.impl HttpSolrClient HttpClientUtil)
           (org.apache.solr.client.solrj.embedded EmbeddedSolrServer)
           (org.apache.solr.common SolrInputDocument)
           (org.apache.solr.client.solrj SolrQuery SolrRequest$METHOD)
           (org.apache.solr.common.params ModifiableSolrParams CursorMarkParams)
           (org.apache.solr.util DateMathParser)))

(def ^:private url-details (atom {}))
(def ^:private credentials-provider (BasicCredentialsProvider.))
(def ^:private credentials (atom {}))

(declare ^:dynamic *connection*)

(def ^:dynamic *trace-fn* nil)

(declare make-param)

(defn trace
  [str]
  (when *trace-fn*
    (*trace-fn* str)))

(defmacro with-trace
  [fn & body]
  `(binding [*trace-fn* ~fn]
     ~@body))

(defn make-basic-credentials
  [name password]
  (UsernamePasswordCredentials. name password))

(defn make-auth-scope
  [host port]
  (AuthScope. host port))

(defn set-credentials
  [uri name password]
  (.setCredentials credentials-provider
                   (make-auth-scope (.getHost uri) (.getPort uri))
                   (make-basic-credentials name password)))

(defn get-url-details
  [url]
  (let [details (get @url-details url)]
    (if details
      details
      (let [[_ scheme name password rest] (re-matches #"(https?)://(.+):(.+)@(.*)" url)
            details (if (and scheme name password rest)
                      {:clean-url (str scheme "://" rest)
                       :password password
                       :name name}
                      {:clean-url url})
            uri (URI. (:clean-url details))]
        (swap! url-details assoc url details)
        (when (and name password)
          (let [host (if (= (.getPort uri) -1)
                       (str (.getScheme uri) "://" (.getHost uri))
                       (str (.getScheme uri) "://" (.getHost uri) ":" (.getPort uri)))]
            ;;(println "**** CLOJURE-SOLR: Adding credentials for host" host)
            (set-credentials uri name password)
            (swap! credentials assoc host {:name name :password password})))
        details))))

(defn connect [url]
  (let [params (ModifiableSolrParams.)
        {:keys [clean-url name password]} (get-url-details url)
        builder (doto (HttpClientBuilder/create)
                  (.setDefaultCredentialsProvider credentials-provider)
                  (.addInterceptorFirst 
                   (reify 
                     HttpRequestInterceptor
                     (^void process [this ^HttpRequest request ^HttpContext context]
                      (let [auth-state (.getAttribute context HttpClientContext/TARGET_AUTH_STATE)]
                        (when (nil? (.getAuthScheme auth-state))
                          (let [target-host (.getAttribute context HttpCoreContext/HTTP_TARGET_HOST)
                                auth-scope (make-auth-scope (.getHostName target-host) (.getPort target-host))
                                creds (.getCredentials credentials-provider auth-scope)]
                            ;;(println "**** CLOJURE-SOLR: HttpRequestInterceptor here.  Looking for" auth-scope)
                            (when creds
                              (.update auth-state (BasicScheme.) creds)))))))))
        client (.build builder)]
    (HttpSolrClient. clean-url client)))

(defn- make-document [boost-map doc]
  (let [sdoc (SolrInputDocument.)]
    (doseq [[key value] doc]
      (let [key-string (name key)
            boost (get boost-map key)]
        (if boost
          (.addField sdoc key-string value boost)
          (.addField sdoc key-string value))))
    sdoc))

(defn add-document!
  ([doc boost-map]
   (.add *connection* (make-document boost-map doc)))
  ([doc]
   (add-document! doc {})))

(defn add-documents!
  ([coll boost-map]
   (.add *connection* (map (partial make-document boost-map) coll)))
  ([coll]
   (add-documents! coll {})))

(defn commit! []
  (.commit *connection*))

(defn- doc-to-hash [doc]
  (into {} (for [[k v] (clojure.lang.PersistentArrayMap/create doc)]
             [(keyword k)
              (cond (= java.util.ArrayList (type v)) (into [] v)
                    :else v)])))

(defn- make-param [p]
  (cond
   (string? p) (into-array String [p])
   (coll? p) (into-array String (map str p))
   :else (into-array String [(str p)])))

(def http-methods {:get SolrRequest$METHOD/GET, :GET SolrRequest$METHOD/GET
                   :post SolrRequest$METHOD/POST, :POST SolrRequest$METHOD/POST})

(defn- parse-method [method]
  (get http-methods method SolrRequest$METHOD/GET))

(defn extract-facets
  [query-results facet-hier-sep limiting? formatters]
  (map (fn [f] {:name (.getName f)
                :values (sort-by :path
                                 (map (fn [v]
                                        (let [result
                                              (merge
                                               {:value (.getName v)
                                                :count (.getCount v)}
                                               (when-let [split-path (and facet-hier-sep (str/split (.getName v) facet-hier-sep))]
                                                 {:split-path split-path
                                                  :title (last split-path)
                                                  :depth (count split-path)}))]
                                          ((get formatters (.getName f) identity) result)))
                                      (.getValues f)))})
     (if limiting?
       (.getLimitingFacets query-results)
       (.getFacetFields query-results))))

(def date-time-formatter
  (tformat/formatters :date-time-no-ms))

(def query-result-date-time-parser
  (tformat/formatter t/utc "YYYY-MM-dd'T'HH:mm:ss.SSS'Z'" "YYYY-MM-dd'T'HH:mm:ss'Z'"))

(defn format-range-value
  "Timezone is only used if it's a date facet (and timezone is not null)."
  [val timezone end?]
  (let [d (try (tformat/parse date-time-formatter val)
               (catch Exception _))]
    (cond (or d (instance? java.util.Date val))
          (let [d (or d (tcoerce/from-date val))]
            (tformat/unparse (if timezone (tformat/with-zone date-time-formatter timezone) date-time-formatter)
                             (if end? (t/minus d (t/seconds 1)) d)))
          :else (str val))))

(defn format-standard-filter-query
  [name value]
  (format "%s:%s" name value))

(defn format-raw-query
  [name value]
  (format "{!raw f=%s}%s" name value))

(defn format-facet-query
  [{:keys [name value formatter]}]
  (if (and (string? value) (re-find #"\[" value)) ;; range filter
    (format-standard-filter-query name value)
    (if formatter
      (formatter name value)
      (format-raw-query name value))))

(defn extract-facet-ranges
  "Explicitly pass facet-date-ranges in case one or more ranges are date ranges, and we need to grab a timezone."
  [query-results facet-date-ranges]
  (sort-by :name
           (map (fn [r]
                  (let [date-range? (some (fn [{:keys [field]}] (= field (.getName r)))
                                          facet-date-ranges)
                        timezone (:timezone (first (filter (fn [{:keys [field]}] (= field (.getName r)))
                                                           facet-date-ranges)))
                        gap (.getGap r)
                        values
                        (map (fn [val]
                               (let [start-val (.getValue val)
                                     start-str (format-range-value start-val nil false)
                                     end-val (cond date-range?
                                                   (.parseMath (doto (DateMathParser.)
                                                                 (.setNow
                                                                  (tcoerce/to-date
                                                                   (tformat/parse query-result-date-time-parser start-val))))
                                                               gap)
                                                   (re-matches #"\d+" start-val)
                                                   (+ (Integer/parseInt start-val) gap)
                                                   :else (+ (Double/parseDouble start-val) gap))
                                     end-str (format-range-value end-val nil true)]
                                 {:count (.getCount val)
                                  :value (format (if date-range? "[%s TO %s]" "[%s TO %s}") start-str end-str)
                                  :min-inclusive start-str
                                  :max-noninclusive end-str}))
                             (.getCounts r))
                        values-before (if (and (.getBefore r) (> (.getBefore r) 0))
                                        (concat [{:count (.getBefore r)
                                                  :value (format (if date-range? "[* TO %s]" "[* TO %s}")
                                                                 (format-range-value (.getStart r) nil true))
                                                  :min-inclusive nil
                                                  :max-noninclusive (format-range-value (.getStart r) timezone true)}]
                                                values)
                                        values)
                        values-before-after (if (and (.getAfter r) (> (.getAfter r) 0))
                                              (concat values-before
                                                      [{:count (.getAfter r)
                                                        :value (format "[%s TO *]"
                                                                       (format-range-value (.getEnd r) nil false))
                                                        :min-inclusive (format-range-value (.getEnd r) timezone false)
                                                        :max-noninclusive nil}])
                                              values-before)]
                    {:name   (.getName r)
                     :values (map #(dissoc % :orig-value) values-before-after)
                     :start  (.getStart r)
                     :end    (.getEnd r)
                     :gap    (.getGap r)
                     :before (.getBefore r)
                     :after  (.getAfter r)}))
                (.getFacetRanges query-results))))

(defn extract-facet-queries
  [facet-queries result]
  (filter identity
          (map (fn [query]
                 (let [formatted-query (if (string? query) query (format-facet-query query))
                       count (get result formatted-query)]
                   (when count
                     (if (string? query)
                       {:query query :count count}
                       (assoc query :count count)))))
               facet-queries)))

(defn extract-pivots
  [query-results facet-date-ranges]
  (let [facet-pivot (.getFacetPivot query-results)]
    (when facet-pivot
      (merge
       (into
        {}
        (map
         (fn [index]
           (let [facet1-name (.getName facet-pivot index)
                 pivot-fields (.getVal facet-pivot index)
                 ranges (into {}
                              (for [pivot-field pivot-fields]
                                (let [facet1-value (.getValue pivot-field)
                                      facet-ranges (extract-facet-ranges pivot-field facet-date-ranges)]
                                  [facet1-value
                                   (into {}
                                         (map (fn [range]
                                                [(:name range) (:values range)])
                                              facet-ranges))])))]
             [facet1-name ranges]))
         (range 0 (.size facet-pivot))))
       (into
        {}
        (map
         (fn [index]
           (let [facet1-name (.getName facet-pivot index)
                 pivot-fields (.getVal facet-pivot index)
                 pivots (into {}
                              (for [pivot-field pivot-fields]
                                (let [facet1-value (.getValue pivot-field)
                                      pivot-values (.getPivot pivot-field)]
                                  [facet1-value
                                   (into {}
                                         (map (fn [pivot]
                                                [(.getValue pivot) (.getCount pivot)])
                                              pivot-values))])))]
             ;;(println facet1-name)
             [facet1-name pivots]))
         (range 0 (.size facet-pivot))))
       ))))

(defn show-query
  [q flags]
  (trace "Solr Query:")
  (trace q)
  (trace "  Facet filters:")
  (if (not-empty (:facet-filters flags))
    (doseq [ff (:facet-filters flags)]
      (trace (format "    %s" (format-facet-query ff))))
    (trace "    none"))
  (trace "  Facet queries:")
  (if (not-empty (:facet-queries flags))
    (doseq [ff (:facet-queries flags)]
      (trace (format "    %s" (format-facet-query ff))))
    (trace "    none"))
  (trace "  Facet fields:")
  (if (not-empty (:facet-fields flags))
    (doseq [ff (:facet-fields flags)]
      (trace (format "    %s" (if (map? ff) (:name ff) ff))))
    (trace "    none"))
  )

(defn search*
  "Query solr through solrj.
   q: Query field
   Optional keys, passed in a map:
     :method                :get or :post (default :get)
     :rows                  Number of rows to return (default is Solr default: 1000)
     :start                 Offset into query result at which to start returning rows (default 0)
     :fields                Fields to return
     :facet-fields          Discrete-valued fields to facet.  Can be a string, keyword, or map containing
                            {:name ... :prefix ...}.
     :facet-queries         Vector of facet queries, each encoded in a string or a map of
                            {:name, :value, :formatter}.  :formatter is optional and defaults to the raw query formatter.
                            The result is in the :facet-queries response.
     :facet-date-ranges     Date fields to facet as a vector or maps.  Each map contains
                             :field   Field name
                             :tag     Optional, for referencing in a pivot facet
                             :start   Earliest date (as java.util.Date)
                             :end     Latest date (as java.util.Date)
                             :gap     Faceting gap, as String, per Solr (+1HOUR, etc)
                             :others  Comma-separated string: before,after,between,none,all.  Optional.
                             :include Comma-separated string: lower,upper,edge,outer,all.  Optional.
                             :hardend Boolean (See Solr doc).  Optional.
                             :missing Boolean--return empty buckets if true.  Optional.
     :facet-numeric-ranges  Numeric fields to facet, as a vector of maps.  Map fields as for
                            date ranges, but start, end and gap must be numbers.
     :facet-mincount        Minimum number of docs in a facet for the bucket to be returned.
     :facet-hier-sep        Useful for path hierarchy token faceting.  A regex, such as \\|.
     :facet-filters         Solr filter expression on facet values.  Passed as a map in the form:
                            {:name 'facet-name' :value 'facet-value' :formatter (fn [name value] ...) }
                            where :formatter is optional and is used to format the query.
     :facet-pivot-fields    Vector of pivots to compute, each a list of facet fields.
                            If a facet is tagged (e.g., {:tag ts} in :facet-date-ranges)  
                            then the string should be {!range=ts}other-facet.  Otherwise,
                            use comma separated lists: this-facet,other-facet.
     :cursor-mark           true -- initial cursor; else a previous cursor value from 
                            (:next-cursor-mark (meta result))
  Additional keys can be passed, using Solr parameter names as keywords.
  Returns the query results as the value of the call, with faceting results as metadata.
  Use (meta result) to get facet data."
  [q {:keys [method fields facet-fields facet-date-ranges facet-numeric-ranges facet-queries
             facet-mincount facet-hier-sep facet-filters facet-pivot-fields cursor-mark] :as flags}]
  (show-query q flags)
  (let [query (SolrQuery. q)
        method (parse-method method)
        facet-result-formatters (into {} (map #(if (map? %)
                                                 [(:name %) (:result-formatter % identity)]
                                                 [% identity])
                                              facet-fields))]
    (doseq [[key value] (dissoc flags :method :facet-fields :facet-date-ranges :facet-numeric-ranges :facet-filters)]
      (.setParam query (apply str (rest (str key))) (make-param value)))
    (when (not (empty? fields))
      (cond (string? fields)
            (.setFields query (into-array (str/split fields #",")))
            (or (seq? fields) (vector? fields))
            (.setFields query (into-array
                               (map (fn [f]
                                      (cond (string? f) f
                                            (keyword? f) (name f)
                                            :else (throw (Exception. (format "Unsupported field name: %s" f)))))
                                    fields)))
            :else (throw (Exception. (format "Unsupported :fields parameter format: %s" fields)))))
    (.addFacetField query
                    (into-array String
                                (map #(if (map? %) (:name %) (name %)) facet-fields)))
    (doseq [facet-field facet-fields]
      (when (map? facet-field)
        (doseq [[key val] facet-field]
          (.setParam query (format "f.%s.facet.%s" (:name facet-field) (name key))
                     (into-array String [(str val)])))))
    (doseq [facet-query facet-queries]
      (cond (string? facet-query)
            (.addFacetQuery query facet-query)
            (map? facet-query)
            (let [formatted-query (format-facet-query facet-query)]
              (when (not-empty formatted-query) (.addFacetQuery query formatted-query)))
            :else (throw (Exception. "Invalid facet query.  Must be a string or a map of {:name, :value, :formatter (optional)}"))))
    (doseq [{:keys [field start end gap others include hardend missing mincount tag]} facet-date-ranges]
      (if tag
        ;; This is a workaround for a Solrj bug that causes tagged queries to be improperly formatted.
        (do (.setParam query "facet" true)
            (.add query "facet.range" (into-array String [(format "{!tag=%s}%s" tag field)]))
            (.add query (format "f.%s.facet.range.start" field)
                  (into-array String [(tformat/unparse (tformat/formatters :date-time-no-ms)
                                                         (tcoerce/from-date start))]))
            (.add query (format "f.%s.facet.range.end" field)
                  (into-array String [(tformat/unparse (tformat/formatters :date-time-no-ms)
                                                         (tcoerce/from-date end))]))
            (.add query (format "f.%s.facet.range.gap" field) (into-array String [gap])))
        (.addDateRangeFacet query field start end gap))
      (when missing (.setParam query (format "f.%s.facet.missing" field) true))
      (when others (.setParam query (format "f.%s.facet.range.other" field) (into-array String others)))
      (when include (.setParam query (format "f.%s.facet.range.include" field) (into-array String [include])))
      (when hardend (.setParam query (format "f.%s.facet.range.hardend" field) hardend)))
    (doseq [{:keys [field start end gap others include hardend missing mincount tag]} facet-numeric-ranges]
      (assert (instance? Number start))
      (assert (instance? Number end))
      (assert (instance? Number gap))
      (if tag
        ;; This is a workaround for a Solrj bug that causes tagged queries to be improperly formatted.
        (do (.setParam query "facet" true)
            (.add query "facet.range" (into-array String [(format "{!tag=%s}%s" tag field)]))
            (.add query (format "f.%s.facet.range.start" field) (into-array String [(.toString start)]))
            (.add query (format "f.%s.facet.range.end" field) (into-array String [(.toString end)]))
            (.add query (format "f.%s.facet.range.gap" field) (into-array String [(.toString gap)])))
        (.addNumericRangeFacet query field start end gap))
      (when missing (.setParam query (format "f.%s.facet.missing" field) true))
      (when others (.setParam query (format "f.%s.facet.range.other" field) (into-array String others)))
      (when include (.setParam query (format "f.%s.facet.range.include" field) (into-array String [include])))
      (when hardend (.setParam query (format "f.%s.facet.range.hardend" field) hardend)))
    (doseq [field facet-pivot-fields]
      (.addFacetPivotField query (into-array String [field])))
    (.addFilterQuery query (into-array String (filter not-empty (map format-facet-query facet-filters))))
    (.setFacetMinCount query (or facet-mincount 1))
    (cond (= cursor-mark true)
          (.setParam query ^String (CursorMarkParams/CURSOR_MARK_PARAM) (into-array String [(CursorMarkParams/CURSOR_MARK_START)]))
          (not (nil? cursor-mark))
          (.setParam query ^String (CursorMarkParams/CURSOR_MARK_PARAM) (into-array String [cursor-mark])))
    (trace "Executing query")
    (let [query-results (.query *connection* query method)
          results (.getResults query-results)]
      (trace "Query complete")
      (with-meta (map doc-to-hash results)
        (merge
         (when cursor-mark
           (let [next  (.getNextCursorMark query-results)]
             {:next-cursor-mark next
              :cursor-done (.equals next (if (= cursor-mark true)
                                           (CursorMarkParams/CURSOR_MARK_START)
                                           cursor-mark))}))
         {:start (.getStart results)
          :rows-set (count results)
          :rows-total (.getNumFound results)
          :highlighting (.getHighlighting query-results)
          :facet-fields (extract-facets query-results facet-hier-sep false facet-result-formatters)
          :facet-range-fields (extract-facet-ranges query-results facet-date-ranges)
          :limiting-facet-fields (extract-facets query-results facet-hier-sep true facet-result-formatters)
          :facet-queries (extract-facet-queries facet-queries (.getFacetQuery query-results))
          :facet-pivot-fields (extract-pivots query-results facet-date-ranges)
          :results-obj results
          :query-results-obj query-results})))))
  
(defn search
  "Query solr through solrj.
   q: Query field
   Optional keys:
     :method                :get or :post (default :get)
     :rows                  Number of rows to return (default is Solr default: 1000)
     :start                 Offset into query result at which to start returning rows (default 0)
     :fields                Fields to return
     :facet-fields          Discrete-valued fields to facet.  Can be a string, keyword, or map containing
                            {:name ... :prefix ...}.
     :facet-queries         Vector of facet queries, each encoded in a string or a map of
                            {:name, :value, :formatter}.  :formatter is optional and defaults to the raw query formatter.
                            The result is in the :facet-queries response.
     :facet-date-ranges     Date fields to facet as a vector or maps.  Each map contains
                             :field   Field name
                             :tag     Optional, for referencing in a pivot facet
                             :start   Earliest date (as java.util.Date)
                             :end     Latest date (as java.util.Date)
                             :gap     Faceting gap, as String, per Solr (+1HOUR, etc)
                             :others  Comma-separated string: before,after,between,none,all.  Optional.
                             :include Comma-separated string: lower,upper,edge,outer,all.  Optional.
                             :hardend Boolean (See Solr doc).  Optional.
                             :missing Boolean--return empty buckets if true.  Optional.
     :facet-numeric-ranges  Numeric fields to facet, as a vector of maps.  Map fields as for
                            date ranges, but start, end and gap must be numbers.
     :facet-mincount        Minimum number of docs in a facet for the bucket to be returned.
     :facet-hier-sep        Useful for path hierarchy token faceting.  A regex, such as \\|.
     :facet-filters         Solr filter expression on facet values.  Passed as a map in the form:
                            {:name 'facet-name' :value 'facet-value' :formatter (fn [name value] ...) }
                            where :formatter is optional and is used to format the query.
     :facet-pivot-fields    Vector of pivots to compute, each a list of facet fields.
                            If a facet is tagged (e.g., {:tag ts} in :facet-date-ranges)  
                            then the string should be {!range=ts}other-facet.  Otherwise,
                            use comma separated lists: this-facet,other-facet.
     :cursor-mark           true -- initial cursor; else a previous cursor value from 
                            (:next-cursor-mark (meta result))
  Returns the query results as the value of the call, with faceting results as metadata.
  Use (meta result) to get facet data."
  [q & {:keys [method fields facet-fields facet-date-ranges facet-numeric-ranges facet-queries
               facet-mincount facet-hier-sep facet-filters facet-pivot-fields] :as flags}]
  (search* q flags))

(defn atomically-update!
  "Atomically update a solr document:
    doc: document fetched from solr previously, or the id of such a document (must not be a map)
    unique-key: Name of the attribute that is the document's unique key
    changes: Vector of maps containing :attribute, :func [one of :set, :inc, :add], and :value.
   e.g.
     (atomically-update! doc \"cdid\" [{:attribute :client :func :set :value \"darcy\"}])"
  [doc unique-key-field changes]
  (let [document (SolrInputDocument.)]
    (.addField document (name unique-key-field) (if (map? doc) (get doc unique-key-field) doc))
    (doseq [{:keys [attribute func value]} changes]
      (.addField document (name attribute) (doto (HashMap. 1) (.put (name func) value))))
    (.add *connection* document)))
  

(defn similar [doc similar-count & {:keys [method]}]
  (let [query (SolrQuery. (format "id:%d" (:id doc)))
        method (parse-method method)]
    (.setParam query "mlt" (make-param true))
    (.setParam query "mlt.fl" (make-param "fulltext"))
    (.setParam query "mlt.count" (make-param similar-count))
    (let [query-results (.query *connection* query method)]
      (map doc-to-hash (.get (.get (.getResponse query-results) "moreLikeThis") (str (:id doc)))))))

(defn delete-id! [id]
  (.deleteById *connection* id))

(defn delete-query! [q]
  (.deleteByQuery *connection* q))

(defn data-import [type]
  (let [type (cond (= type :full) "full-import"
                   (= type :delta) "delta-import")
        params (doto (ModifiableSolrParams.)
                 (.set "qt" (make-param "/dataimport"))
                 (.set "command" (make-param type)))]
    (.query *connection* params)))

(defmacro with-connection [conn & body]
  `(binding [*connection* ~conn]
     (try
       (do ~@body)
       (finally (.close *connection*)))))

