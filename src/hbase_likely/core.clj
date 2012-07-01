;;
(ns hbase-likely.core
  (:require (cascalog [workflow :as w]
                      [ops :as c]
                      [vars :as v]))
  (:import (java.util Set ArrayList)
           (org.apache.hadoop.hbase HBaseConfiguration)
           (org.apache.hadoop.conf Configuration)
           (org.apache.hadoop.hbase.client Put Get HTable Scan)
           (org.apache.hadoop.hbase.util Bytes)
           (com.twitter.maple.hbase HBaseTap HBaseScheme)
           (cascading.tuple Fields)
           (cascading.hbase ByteHolder)
           (com.cybozu.labs.langdetect Detector DetectorFactory Language)))

(defn -main
  "Populate data tables with languages of tweets and full urls
   Assumes that:
     create 'urls', 'url'
   and
     create 'language', 'lang'
   have been run in the hbase shell.
   This may take some time to run..."
  []
  (process-languages "tweets" "base" "content")
  (process-language-probabilities "tweets" "base" "content")
  (split-urls "tweets" "base"))

(defn hbase-table [table-name]
  ;; Note that (HBaseConfiguration.) is deprecated in HBase 0.95-SNAPSHOT
  ;; and will be replaced by (Configuration/create)
  (HTable. (HBaseConfiguration.) table-name))

(defn hbase-scan 
  "Returns a scan object with added column"
  [column-family qualifier]
  (.addColumn (Scan.) (Bytes/toBytes column-family) (Bytes/toBytes qualifier)))

(defn hbase-scans
  "Returns a scan object with added columns"
  [column-family qualifiers]
  (let [s (Scan.)]
    (doseq [q qualifiers]
      (.addColumn s (Bytes/toBytes column-family) (Bytes/toBytes q)))
    s))

(defn hbase-results 
  "Returns a ResultScanner for a table and scanner object"
  [table scanner]
  (.getScanner table scanner))

;; Detect the language
(defn load-detector-factory-profile []
  (DetectorFactory/loadProfile "/home/hduser/src/java/langdetect/profiles/"))

(defn detect-language
  "Detect the most probable language of a string"
  [string]
  (let [detector (DetectorFactory/create)]
    (.append detector string)
      (String. (.detect detector))))

(defn detect-language-probabilities
  "Return an seq of com.cybozu.labs.langdetect.Language containing the possible
   languages of a string along with their probabilities"
  [string]
  (let [detector (DetectorFactory/create)]
    (.append detector string)
    (seq (.getProbabilities detector))))

(defn lang-detect-rs
  "Iterates over a result set and detects language"
  [results table column-family qualifier]
  (let [iterator (.iterator results)]
    (while (.hasNext iterator)
      (let [row (.next iterator)
            row-id (String. (.getRow row))
            put-data (Put. (Bytes/toBytes row-id))]
        (try
          (let [lang (detect-language
                  (String. (.getValue row
                                      (Bytes/toBytes column-family)
                                      (Bytes/toBytes qualifier))))]
            (.add put-data (Bytes/toBytes column-family)
                  (Bytes/toBytes "language")
                  (Bytes/toBytes lang))
            (.put table put-data))
          (catch Exception e
            (.add put-data (Bytes/toBytes column-family)
                  (Bytes/toBytes "language")
                  (Bytes/toBytes "nil"))
            0))))))

(defn split-lang-prob
  "Splits a Language string into language and probability"
  [language]
  (seq (.split #":" (.toString language))))

(defn lang-prob-rs
  "Iterates over a result set and detects language probabilities"
  [results table lang-table column-family qualifier]
  (let [iterator (.iterator results)]
    (while (.hasNext iterator)
      (let [row (.next iterator)
            row-id (String. (.getRow row))
            put-data (Put. (Bytes/toBytes row-id))]
        (try
          (let [lang (detect-language-probabilities
                  (String. (.getValue row
                                      (Bytes/toBytes column-family)
                                      (Bytes/toBytes qualifier))))
            ;; Each language and probability in the array needs to be added
            ;; to the 'language' table. The qualifier for the table
                ;; is the language and the value is the probability
            lang-prob (map #(split-lang-prob %) lang)]
            (doseq [lp lang-prob]
			  (let [lang (first lp)
			       prob (second lp)]
			       (.add put-data (Bytes/toBytes "lang")
                  (Bytes/toBytes lang)
                  (Bytes/toBytes prob))))
            (.put lang-table put-data))
          (catch Exception e
            ;; If no language is detected add 'nil' to 'other' qualifier
            ;; in language table
            (.add put-data (Bytes/toBytes "lang")
                  (Bytes/toBytes "other")
                  (Bytes/toBytes "nil"))
            (.put lang-table put-data)
            0))))))

(defn process-languages
  "Detect languages for every tweet"
  [table-name column-family qualifier]
  (let [table (hbase-table table-name)
        scan (hbase-scan column-family qualifier)
        rs (hbase-results table scan)]
    (load-detector-factory-profile)
    (lang-detect-rs rs table column-family qualifier)))

;; (process-languages "tweets_copy" "base" "content")

(defn process-language-probabilities
  "Detect language probabilities for every tweet"
  [table-name column-family qualifier]
  (let [table (hbase-table table-name)
        lang-table (hbase-table "language")
        scan (hbase-scan column-family qualifier)
        rs (hbase-results table scan)]
    (load-detector-factory-profile)
    (lang-prob-rs rs table lang-table column-family qualifier)))

;; (process-language-probabilities "tweets_copy" "base" "content")

(defn strip-url
  "Strips a split url of square braces and quotes"
  [url]
  (let [url1 (.replaceAll url "\\x5B" "")
        url2 (.replaceAll url1 "\\x5D" "")]
    ;; The double quote character is hex 22
    (.replaceAll url2 "\\x22" "")))

(defn add-url
  "Takes a string url and adds it to the database"
  [short-url full-url screen-name tweet-id]
  (let [url-table (hbase-table "urls")
        s-url (strip-url short-url)
        split-url (second (.split s-url "\\x2F\\x2F"))
        row-id (str tweet-id "-" split-url)
        p (Put. (Bytes/toBytes row-id))
        cf (Bytes/toBytes "url")]
    (doto p
      (.add cf (Bytes/toBytes "screen_name") (Bytes/toBytes screen-name))
      (.add cf (Bytes/toBytes "tweet_id") (Bytes/toBytes tweet-id))
      (.add cf (Bytes/toBytes "short_url") (Bytes/toBytes s-url))
      (.add cf (Bytes/toBytes "full_url") (Bytes/toBytes full-url)))
    (.put url-table p)))
     
(defn lookup-url
  "Takes url string and looks it up in short_url table"
  [url-str]
  (let [stripped (strip-url url-str)
        g (Get. (Bytes/toBytes stripped))
        table-name "short_urls"
        cf (Bytes/toBytes "rel")
        q (Bytes/toBytes "url")
        url-table (hbase-table table-name)]
    (.addColumn g cf q) 
    (let [result (.get url-table g)]
      (if (.containsColumn result cf q)
        (String. (.getValue result cf q))
        ;; If the full-url can't be found just return the stripped
        ;; original url string
        stripped))))

(defn tweet-iterator-seq
  "Returns a lazy sequence of rows from the resultset iterator"
  [iterator]
  (lazy-seq
    (when (.hasNext iterator)
      (cons (.next iterator) (tweet-iterator-seq iterator)))))

(defn url-rs-seq
  "Iterate over a lazy-seq of urls"
  [rs column-family qualifiers]
  (let [iterator (.iterator rs)
        row-seq (tweet-iterator-seq iterator)]
    (for [row row-seq]
      (let [row-id (String. (.getRow row))
            screen-name (String. (.getValue row
                                     (Bytes/toBytes column-family)
                                     (Bytes/toBytes (first qualifiers))))]
        ;; Check if there are actually urls present in the tweet
        (if (.containsColumn row
                           (Bytes/toBytes column-family)
                           (Bytes/toBytes (second qualifiers)))
          (let [urls (String. (.getValue row
                                         (Bytes/toBytes column-family)
                                         (Bytes/toBytes (second qualifiers))))
                url-seq (seq (.split urls ","))]
            (map #(add-url % (lookup-url %) screen-name row-id) url-seq))
          ;; If no urls are present, no need to add them
          (println (str "Warning: no urls present in row: " row-id)))))))

(defn split-urls
  "Split multiple urls and place into a new table"
  [table-name column-family]
  (let [qualifiers ["screen_name" "urls"]
        table (hbase-table table-name)
        scan (hbase-scans column-family qualifiers)
        rs (hbase-results table scan)]
    (url-rs-seq rs column-family qualifiers)))

;; Stunningly slow!
;;(split-urls "tweets_copy" "base")