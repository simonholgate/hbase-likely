;;
(ns hbase-likely.hbase
  (:require (cascalog [workflow :as w]
                      [ops :as c]
                      [vars :as v]))
  (:import (org.apache.hadoop.hbase.util Bytes)
           (com.twitter.maple.hbase HBaseTap HBaseScheme)
           (cascading.hbase ByteHolder)))
           
(use '[cascalog api playground])
(bootstrap-emacs)                       

;; (defn hbase-tap [table-name key-field column-family & value-fields]
;;   (let [scheme (HBaseScheme. (w/fields key-field) column-family (w/fields value-fields))]
;;     (HBaseTap. table-name scheme)))

;; (defn as-string [^ByteHolder bytes]
;;     (.toString bytes))

;; (let [age-table (hbase-tap "age-table" "?person" "cf" "?age")]
;;   (?<- (stdout) [?p ?a] (age-table ?p ?age) (as-string ?age :> ?a)))

(defn hbase-tap [table-name key-field column-family & value-fields]
  (let [scheme (HBaseScheme. (w/fields key-field) column-family (w/fields value-fields))]
    (HBaseTap. table-name scheme)))

(defn as-string [^ByteHolder bytes]
    (.toString bytes))

(defn to-string [bytes]
  (String. bytes))

(defn to-split-string [bytes]  
  (seq (.split #"\," (String. bytes))))

(defn to-int [bytes]
  (Integer/parseInt (String. bytes)))

(defmapcatop split
  "Splits a 'sentence' on non-word characters"
  [sentence]
  (seq (.split sentence "\\s+")))

(defmapcatop split-urls
  "Splits a set of comma separated urls"
  [urls]
  (seq (.split #"\\," urls)))

(defmapcatop strip-urls
  "Strips a split url of square braces and quotes"
  [url]
  (let [url1 (.replaceAll url "\\x5B" "")
        url2 (.replaceAll url1 "\\x5D" "")]
    ;; The double quote character is hex 22
    (.replaceAll url2 "\\x22" "")))


(defn lowercase [w] (.toLowerCase w))
