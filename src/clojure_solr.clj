(ns clojure-solr
  (:require [clojure.string :as str])
  (:require [clj-time.core :as t])
  (:require [clj-time.format :as tformat])
  (:require [clj-time.coerce :as tcoerce])
  (:import (org.apache.solr.client.solrj.impl HttpSolrClient)
           (org.apache.solr.common SolrInputDocument)
           (org.apache.solr.client.solrj SolrQuery SolrRequest$METHOD)
           (org.apache.solr.common.params ModifiableSolrParams)
           (org.apache.solr.util DateMathParser)))

(declare ^:dynamic *connection*)

(defn connect [url]
  (HttpSolrClient. url))

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
                                  :value (format "[%s TO %s]" start-str end-str)
                                  :min-inclusive start-str
                                  :max-noninclusive end-str}))
                             (.getCounts r))
                        values-before (if (and (.getBefore r) (> (.getBefore r) 0))
                                        (concat [{:count (.getBefore r)
                                                  :value (format "[* TO %s]" (format-range-value (.getStart r) nil true))
                                                  :min-inclusive nil
                                                  :max-noninclusive (format-range-value (.getStart r) timezone true)}]
                                                values)
                                        values)
                        values-before-after (if (and (.getAfter r) (> (.getAfter r) 0))
                                              (concat values-before
                                                      [{:count (.getAfter r)
                                                        :value (format "[%s TO *]" (format-range-value (.getEnd r) nil false))
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

(defn extract-pivots
  [query-results facet-date-ranges]
  (let [facet-pivot (.getFacetPivot query-results)]
    (when facet-pivot
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
        (range 0 (.size facet-pivot)))))))

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
     :facet-queries         Vector of facet queries, each encoded in a string.
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
  Returns the query results as the value of the call, with faceting results as metadata.
  Use (meta result) to get facet data."
  [q & {:keys [method fields facet-fields facet-date-ranges facet-numeric-ranges facet-queries
               facet-mincount facet-hier-sep facet-filters facet-pivot-fields] :as flags}]
  (let [query (SolrQuery. q)
        method (parse-method method)
        facet-result-formatters (into {} (map #(if (map? %)
                                                 [(:name %) (:result-formatter % identity)]
                                                 [% identity])
                                              facet-fields))]
    (doseq [[key value] (dissoc flags :method :facet-fields :facet-date-ranges :facet-numeric-ranges :facet-filters)]
      (.setParam query (apply str (rest (str key))) (make-param value)))
    (when (not (empty? fields))
      (.setFields query (into-array (map name fields))))
    (.addFacetField query
                    (into-array String
                                (map #(if (map? %) (:name %) (name %)) facet-fields)))
    (doseq [facet-field facet-fields]
      (when (map? facet-field)
        (if (:prefix facet-field)
          (.setParam query (format "f.%s.facet.prefix" (:name facet-field)) (into-array String [(:prefix facet-field)])))))
    (doseq [facet-query facet-queries]
      (.addFacetQuery query facet-query))
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
    (.addFilterQuery query (into-array String (map (fn [{:keys [name value formatter]}]
                                                     (if (re-find #"\[" value) ;; range filter
                                                       (format "%s:%s" name value)
                                                       (if formatter
                                                         (formatter name value)
                                                         (format-raw-query name value))))
                                                 facet-filters)))
    (.setFacetMinCount query (or facet-mincount 1))
    (let [query-results (.query *connection* query method)
          results (.getResults query-results)]
      (with-meta (map doc-to-hash results)
        {:start (.getStart results)
         :rows-set (count results)
         :rows-total (.getNumFound results)
         :highlighting (.getHighlighting query-results)
         :facet-fields (extract-facets query-results facet-hier-sep false facet-result-formatters)
         :facet-range-fields (extract-facet-ranges query-results facet-date-ranges)
         :limiting-facet-fields (extract-facets query-results facet-hier-sep true facet-result-formatters)
         :facet-pivot-fields (extract-pivots query-results facet-date-ranges)
         :results-obj results
         :query-results-obj query-results}))))

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
     ~@body))
