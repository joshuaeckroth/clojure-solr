(ns clojure-solr-test
  (:import (org.apache.solr.client.solrj.embedded EmbeddedSolrServer))
  (:import (org.apache.solr.core CoreContainer))
  (:require [clj-time.core :as t])
  (:require [clj-time.coerce :as tcoerce])
  (:use [clojure.test])
  (:use [clojure-solr]))

(defn solr-server-fixture
  [f]
  (System/setProperty "solr.solr.home" "test-files")
  (System/setProperty "solr.dist.dir" (str (System/getProperty "user.dir")
                                           "/test-files/dist"))
  (let [cont (CoreContainer.)]
    (.load cont)
    (binding [*connection* (EmbeddedSolrServer. cont "clojure-solr")]
      (f)
      (.shutdown *connection*))))

(use-fixtures :each solr-server-fixture)

(def sample-doc
  {:id "1" :type "pdf" :title "my title" :fulltext "my fulltext" :numeric 10
   :updated (tcoerce/to-date (t/date-time 2015 02 27))
   :terms ["Vocabulary 1/Term A" "Vocabulary 1/Term B" "Vocabulary 2/Term X/Term Y"]})

(deftest test-add-document!
  (do (add-document! sample-doc)
      (commit!))
  (is (= sample-doc (first (search "my"))))
  (is (= {:start 0 :rows-set 1 :rows-total 1} (select-keys (meta (search "my"))
                                                           [:start :rows-set :rows-total])))
  (is (= [{:name "terms"
           :values
           [{:value "Vocabulary 1" :split-path ["Vocabulary 1"] :title "Vocabulary 1" :depth 1 :count 1}
            {:value "Vocabulary 1/Term A" :split-path ["Vocabulary 1" "Term A"] :title "Term A" :depth 2 :count 1}
            {:value "Vocabulary 1/Term B" :split-path ["Vocabulary 1" "Term B"] :title "Term B" :depth 2 :count 1}
            {:value "Vocabulary 2" :split-path ["Vocabulary 2"] :title "Vocabulary 2" :depth 1 :count 1}
            {:value "Vocabulary 2/Term X" :split-path ["Vocabulary 2" "Term X"] :title "Term X" :depth 2 :count 1}
            {:value "Vocabulary 2/Term X/Term Y" :split-path ["Vocabulary 2" "Term X" "Term Y"] :title "Term Y" :depth 3 :count 1}]}]
         (:facet-fields
           (meta (search "my" :facet-fields [:terms] :facet-hier-sep #"/")))))
  (is (= [{:name   "numeric"
           :values [{:value "[*+TO+10]" :title "10" :count 1}]
           :start  10
           :end    12
           :gap    1}
          {:name   "updated"
           :values [{:value "[*+TO+2015-02-27T00:00:00Z]" :title "2015-02-27T00:00:00Z" :count 1}]
           :start  (tcoerce/to-date (t/date-time 2015 02 26))
           :end    (tcoerce/to-date (t/date-time 2015 02 28))
           :gap    "+1DAY"}]
         (:facet-range-fields
           (meta (search "my" :facet-numeric-ranges [{:field "numeric" :start (Integer. 10) :end (Integer. 12) :gap (Integer. 1)}]
                         :facet-date-ranges [{:field "updated"
                                              :start (tcoerce/to-date (t/date-time 2015 02 26))
                                              :end   (tcoerce/to-date (t/date-time 2015 02 28))
                                              :gap   "+1DAY"}]))))))


