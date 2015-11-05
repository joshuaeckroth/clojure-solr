(defproject cc.artifice/clojure-solr "0.9.0"
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.apache.solr/solr-solrj "4.10.4"]
                 [org.apache.solr/solr-core "4.10.4" :exclusions [commons-fileupload]]
                 [commons-fileupload "1.3"]
                 [org.slf4j/slf4j-jcl "1.7.6"]
                 [clj-time "0.9.0"]]
  :profiles {:dev {:dependencies [[javax.servlet/servlet-api "2.5"]]}}
  ;;:repositories [["restlet" {:url "http://maven.restlet.org"}]]
  :repositories [["restlet" {:url "http://repository.sonatype.org/content/groups/forge"}]]
  )
