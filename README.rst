============
clojure-solr
============

Clojure bindings for `Apache Solr <http://lucene.apache.org/solr/>`_.

Installation
============

To use within a Leiningen project, add the following to your
project.clj file:

::

    [cc.artifice/clojure-solr "1.5.3"]

To build from source, run:

::

    lein deps
    lein jar

Usage
=====

::

  (with-connection (connect "http://127.0.0.1:8983/solr")
    (add-document! {"id" "testdoc", "name" "A Test Document"})
    (add-documents! [{"id" "testdoc.2", "name" "Another test"}
                                 {"id" "testdoc.3", "name" "a final test"}])
    (commit!)
    (search "test")
    (search "test" :rows 2))
