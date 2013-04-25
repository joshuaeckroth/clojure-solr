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
      (f)
      (.shutdown *connection*))))

(use-fixtures :each solr-server-fixture)

(def sample-doc
  {:id "1" :type "pdf" :title "my title" :fulltext "my fulltext"
   :terms ["Vocabulary 1/Term A" "Vocabulary 1/Term B" "Vocabulary 2/Term X/Term Y"]})

(deftest test-add-document!
  (do (add-document! sample-doc)
      (commit!))
  (is (= sample-doc (first (search "my"))))
  (is (= {:start 0 :rows-set 1 :rows-total 1} (select-keys (meta (search "my"))
                                                           [:start :rows-set :rows-total])))
  (is (= [{:name "terms"
           :values
           [{:path "Vocabulary 1" :split-path ["Vocabulary 1"] :name "Vocabulary 1" :depth 1 :count 1}
            {:path "Vocabulary 1/Term A" :split-path ["Vocabulary 1" "Term A"] :name "Term A" :depth 2 :count 1}
            {:path "Vocabulary 1/Term B" :split-path ["Vocabulary 1" "Term B"] :name "Term B" :depth 2 :count 1}
            {:path "Vocabulary 2" :split-path ["Vocabulary 2"] :name "Vocabulary 2" :depth 1 :count 1}
            {:path "Vocabulary 2/Term X" :split-path ["Vocabulary 2" "Term X"] :name "Term X" :depth 2 :count 1}
            {:path "Vocabulary 2/Term X/Term Y" :split-path ["Vocabulary 2" "Term X" "Term Y"] :name "Term Y" :depth 3 :count 1}]}]
         (:facet-fields (meta (search "my" :facet-fields [:terms] :facet-hier-sep #"/"))))))


