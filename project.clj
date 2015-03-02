(defproject cc.artifice/clojure-solr "0.8.12"
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.apache.solr/solr-solrj "4.7.2"]
                 [org.slf4j/slf4j-jcl "1.7.7"]
                 [clj-time "0.9.0"]]
  :profiles {:dev {:dependencies [[org.apache.solr/solr-core "4.7.2"]
                                  [javax.servlet/servlet-api "2.5"]]}}
  :repositories [["restlet" {:url "http://maven.restlet.org"}]])
