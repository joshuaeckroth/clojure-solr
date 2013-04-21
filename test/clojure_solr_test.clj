(ns clojure-solr-test
  (:import (org.apache.solr.client.solrj.embedded EmbeddedSolrServer))
  (:import (org.apache.solr.core CoreContainer CoreContainer$Initializer))
  (:use [clojure.test])
  (:use [clojure-solr]))

(defn solr-server-fixture
  [f]
  (System/setProperty "solr.solr.home" "test-files")
  (System/setProperty "solr.dist.dir" (str (System/getProperty "user.dir")
                                           "/test-files/dist"))
  (let [init (CoreContainer$Initializer.)
        cont (.initialize init)]
    (binding [*connection* (EmbeddedSolrServer. cont "clojure-solr")]
      (f))))

(use-fixtures :each solr-server-fixture)

(deftest test-add-document!
  (do (add-document! {:id 1 :type "pdf" :title "my title" :fulltext "my fulltext"})))

