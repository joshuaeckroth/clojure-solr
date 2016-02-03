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
  [query-results facet-hier-sep limiting?]
  (map (fn [f] {:name (.getName f)
             :values (sort-by :path
                              (map (fn [v]
                                   (let [split-path (str/split (.getName v) facet-hier-sep)]
                                     {:value (.getName v)
                                      :split-path split-path
                                      :title (last split-path)
                                      :depth (count split-path)
                                      :count (.getCount v)}))
                                 (.getValues f)))})
     (if limiting?
       (.getLimitingFacets query-results)
       (.getFacetFields query-results))))

(def date-time-formatter
  (tformat/formatters :date-time-no-ms))

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
                                                                     (tformat/parse date-time-formatter start-val))))
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

(defn search [q & {:keys [method fields facet-fields facet-date-ranges facet-numeric-ranges
                          facet-mincount facet-hier-sep facet-filters] :as flags}]
  (let [query (SolrQuery. q)
        method (parse-method method)]
    (doseq [[key value] (dissoc flags :method :facet-fields :facet-date-ranges :facet-numeric-ranges :facet-filters)]
      (.setParam query (apply str (rest (str key))) (make-param value)))
    (when (not (empty? fields))
      (.setFields query (into-array (map name fields))))
    (.addFacetField query (into-array String (map name facet-fields)))
    (doseq [{:keys [field start end gap others include hardend]} facet-date-ranges]
      (.addDateRangeFacet query field start end gap)
      (when others (.setParam query (format "f.%s.facet.range.other" field) (into-array String others)))
      (when include (.setParam query (format "f.%s.facet.range.include" field) (into-array String [include])))
      (when hardend (.setParam query (format "f.%s.facet.range.hardend" field) hardend)))
    (doseq [{:keys [field start end gap others include hardend]} facet-numeric-ranges]
      (assert (instance? Number start))
      (assert (instance? Number end))
      (assert (instance? Number gap))
      (.addNumericRangeFacet query field start end gap)
      (when others (.setParam query (format "f.%s.facet.range.other" field) (into-array String others)))
      (when include (.setParam query (format "f.%s.facet.range.include" field) (into-array String [include])))
      (when hardend (.setParam query (format "f.%s.facet.range.hardend" field) hardend)))
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
         :facet-fields (extract-facets query-results facet-hier-sep false)
         :facet-range-fields (extract-facet-ranges query-results facet-date-ranges)
         :limiting-facet-fields (extract-facets query-results facet-hier-sep true)
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
