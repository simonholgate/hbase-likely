(defproject hbase-likely "0.1.0"
  :description "Code to connect to HBase for Likely.co project"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.3.0"]
  		 [org.apache.hbase/hbase "0.92.1"]
                 [org.apache.hadoop/hadoop-core "1.0.3"]
                 [com.cybozu.labs.langdetect/langdetect "09.13.2011"]
                 [net.arnx.jsonic/jsonic "1.1.3"]
                 [cascalog "1.9.0"]
                 [cascading/cascading-core "2.0.0"]
                 [com.twitter/maple "0.2.0"]
                 [cascading.hbase "0.0.2"]
                 [org.codehaus.jackson/jackson-core-lgpl "1.9.7"]
                 [org.codehaus.jackson/jackson-mapper-lgpl "1.9.7"]
                 [clojure-csv/clojure-csv "2.0.0-alpha1"]]
  :profiles {:dev {:dependencies [[org.apache.hadoop/hadoop-core "1.0.3"]]}}
  :repositories {"mvnrepository.com" "http://mvnrepository.com/artifact/"
                 "conjars" "http://conjars.org/repo"})
