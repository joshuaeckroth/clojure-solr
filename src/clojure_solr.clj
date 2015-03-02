(ns clojure-solr
  (:require [clojure.string :as str])
  (:import (org.apache.solr.client.solrj.impl HttpSolrServer)
           (org.apache.solr.common SolrInputDocument)
           (org.apache.solr.client.solrj SolrQuery SolrRequest$METHOD)
           (org.apache.solr.common.params ModifiableSolrParams)))

(declare ^:dynamic *connection*)

(defn connect [url]
  (HttpSolrServer. url))

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

(defn extract-facet-ranges
  [query-results]
  (sort-by :name
           (map (fn [r]
                  (let [values (vec (sort-by :value (map (fn [v] {:title (.getValue v) :count (.getCount v)}) (.getCounts r))))
                        values-facet-queries
                        (map (fn [i val]
                               (assoc val :value (-> (format "[%s TO %s]"
                                                             (if (= i 0)
                                                               "*"
                                                               (:title (nth values (dec i))))
                                                             (:title val))
                                                     (str/replace #" " "%20")
                                                     (str/replace #":" "%3A"))))
                             (range (count values)) values)]
                    {:name   (.getName r)
                     :values values-facet-queries
                     :start  (.getStart r)
                     :end    (.getEnd r)
                     :gap    (.getGap r)}))
                (.getFacetRanges query-results))))

(defn search [q & {:keys [method facet-fields facet-date-ranges facet-numeric-ranges
                          facet-hier-sep facet-filters] :as flags}]
  (let [query (SolrQuery. q)
        method (parse-method method)]
    (doseq [[key value] (dissoc flags :method :facet-fields :facet-date-ranges :facet-numeric-ranges :facet-filters)]
      (.setParam query (apply str (rest (str key))) (make-param value)))
    (.addFacetField query (into-array String (map name facet-fields)))
    (doseq [{:keys [field start end gap]} facet-date-ranges]
      (.addDateRangeFacet query field start end gap))
    (doseq [{:keys [field start end gap]} facet-numeric-ranges]
      (assert (instance? Number start))
      (assert (instance? Number end))
      (assert (instance? Number gap))
      (.addNumericRangeFacet query field start end gap))
    (.addFilterQuery query (into-array String (map (fn [{:keys [name value]}]
                                                     (if (re-find #"\[" value) ;; range filter
                                                       (format "%s:%s" name value)
                                                       (format "{!raw f=%s}%s" name value)))
                                                 facet-filters)))
    (.setFacetMinCount query 1)
    (let [query-results (.query *connection* query method)
          results (.getResults query-results)]
      (with-meta (map doc-to-hash results)
        {:start (.getStart results)
         :rows-set (count results)
         :rows-total (.getNumFound results)
         :highlighting (.getHighlighting query-results)
         :facet-fields (extract-facets query-results facet-hier-sep false)
         :facet-range-fields (extract-facet-ranges query-results)
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
