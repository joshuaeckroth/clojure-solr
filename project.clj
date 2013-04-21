(defproject cc.artifice/clojure-solr "0.5.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.apache.solr/solr-solrj "4.2.0"]
                 [org.slf4j/slf4j-jcl "1.6.4"]]
  :profiles {:dev {:dependencies [[org.apache.solr/solr-core "4.2.0"]
                                  [javax.servlet/servlet-api "2.5"]]}})
