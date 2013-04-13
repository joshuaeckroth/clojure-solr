(ns clojure-solr
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
  (clojure.lang.PersistentArrayMap/create doc))

(defn- make-param [p]
  (cond
   (string? p) (into-array String [p])
   (coll? p) (into-array String (map str p))
   :else (into-array String [(str p)])))

(def http-methods {:get SolrRequest$METHOD/GET, :GET SolrRequest$METHOD/GET
              :post SolrRequest$METHOD/POST, :POST SolrRequest$METHOD/POST})

(defn- parse-method [method]
  (get http-methods method SolrRequest$METHOD/GET))

(defn search [q & {:keys [method] :as flags}]
  (let [query (SolrQuery. q)
        method (parse-method method)]
    (doseq [[key value] (dissoc flags :method)]
      (.setParam query (apply str (rest (str key))) (make-param value)))
    (let [results (.getResults (.query *connection* query method))]
      (with-meta (map doc-to-hash results)
        {:start (.getStart results)
         :rows (count results)
         :num-found (.getNumFound results)}))))

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
