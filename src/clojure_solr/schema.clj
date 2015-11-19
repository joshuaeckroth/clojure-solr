(ns clojure-solr.schema
  (:require [clojure.string :as str])
  (:import (org.apache.solr.client.solrj.impl HttpSolrClient)
           (org.apache.solr.client.solrj.request.schema SchemaRequest$Fields)
           (org.apache.solr.client.solrj.response.schema SchemaResponse$FieldsResponse)
           (org.apache.solr.common SolrInputDocument)
           (org.apache.solr.client.solrj SolrQuery SolrRequest$METHOD)
           (org.apache.solr.common.params ModifiableSolrParams)
           (org.apache.solr.util DateMathParser))
  (:require [clojure-solr :as solr]))


(defn get-schema-fields
  []
  (let [generic-response (.request solr/*connection* (SchemaRequest$Fields.))
        fields-response (SchemaResponse$FieldsResponse.)]
    (.setResponse fields-response generic-response)
    (map (fn [field-schema]
           (into {} (map (fn [[k v]] [(keyword k) v]) field-schema)))
         (.getFields fields-response))))

