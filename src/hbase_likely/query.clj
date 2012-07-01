;; Queries on HBase made in Cascalog
(ns hbase-likely.query)
  
(use 'hbase-likely.hbase)
(use '[cascalog api playground])
(require '(cascalog [workflow :as w]
                    [ops :as c]
                    [vars :as v]))
(bootstrap-emacs)

(defbufferop first-tuple [tuples] (take 1 tuples))

(defn distinct-langs
  "How many distinct languages are found?"
  []
  (let [h-table (hbase-tap "tweets_copy" "row-id" "base" "language")]
    (?<- (stdout) [?count] (h-table _ ?language)
         (to-string ?language :> ?l)
         (c/distinct-count ?l :> ?count))))
;; 44!
;; Do we really have a tweet in Tamil? Yep!
(defn tweet-lang
  "Find tweets with a given language, e.g. 'en'"
  [lang-str]
  (let [h-table (hbase-tap "tweets_copy" "row-id" "base"
                           "screen_name" "language")]
     (?<- (stdout) [?l ?r ?s] (h-table ?row-id ?screen-name ?language)
          (to-string ?language :> ?l)
          (= ?l lang-str)
         (to-string ?screen-name :> ?s)
         (to-string ?row-id :> ?r))))

(defn timezone-lang
  "Find the timezone of a tweet with a given language, e.g. 'en'"
  [lang-str]
  (let [h-table (hbase-tap "tweets_copy" "row-id" "base"
                           "screen_name" "language")
        t-table (hbase-tap "twitter_accounts_copy" "row-id" "base"
                           "screen_name" "time_zone")]
    (?<- (stdout) [?l ?r ?s ?t]
         (h-table ?row-id ?screen-name ?language)
          (to-string ?language :> ?l)
          (= ?l lang-str)
          (t-table ?row ?screen-name ?tz)
         (to-string ?screen-name :> ?s)
         (to-string ?row :> ?r)
         (to-string ?tz :> ?t))))

(defn lang-tweet-count
  "How many tweets of each language?"
  []
  (let [h-table (hbase-tap "tweets_copy" "row-id" "base" "language")]
  (?<- (stdout) [?l ?count] (h-table _ ?language)
       (to-string ?language :> ?l)
       (c/count ?count))))

(defn distinct-urls
  "How many different urls are there?"
  []
  (let [h-table (hbase-tap "tweets_copy" "row-id" "base" "urls")
        u-table (hbase-tap "short_urls_copy" "row-id" "rel" "url")]
    ;; Note that there may be multiple urls in a tweet
    ;; Also note that these urls are shortened and will all be different
    ;; so we need to convert them back into full urls
    ;; URLs are comma separated and quoted
    (?<- (stdout) [?count]
         (h-table _ ?urls)
         (to-string ?urls :> ?u)
         (split-urls ?u :> ?usplit)
         (strip-urls ?usplit :> ?ustripped)
         (u-table ?ustripped ?full-url)
         (c/distinct-count ?ustripped :> ?count))))
;; 128759 urls without inner join. But of course the same URL may be shortened
;; in a different way so need to link to short_urls table.

(defn distinct-full-urls
  "How many different full urls are there?"
  []
  (let [u-table (hbase-tap "short_urls_copy" "row-id" "rel" "url")]
    (?<- (stdout) [?count]
         (u-table ?ustripped ?full-url)
         (c/distinct-count ?full-url :> ?count))))
;; 128759 urls in tweets. 147548 distinct full urls. What gives? Multiple
;; urls in the same tweet is one problem.

(defn distinct-full-urls-2
  "How many different full urls are there in the urls table?"
  []
  (let [u-table (hbase-tap "urls" "row-id" "url" "full_url")]
    (?<- (stdout) [?count]
         (u-table ?row-id ?full-url)
         (to-string ?full-url :> ?u)
         (c/distinct-count ?u :> ?count))))
;; 43017 - now they have been split down properly.

(defn url-tweet-count
  "How many tweets of each url are there?"
  []
  (let [u-table (hbase-tap "urls" "row-id" "url" "full_url")]
    (?<- (stdout) [?u ?count]
         (u-table ?row ?url)
         (to-string ?url :> ?u)
         (c/count ?count))))

(defn url-tweet-count-2
  "How many times does each person tweet a url??"
  []
  (let [h-table (hbase-tap "tweets_copy" "row-id" "base" "screen_name")
        u-table (hbase-tap "urls" "row-id" "url" "screen_name")]
    (?<- (stdout) [?sn ?most]
         (h-table ?hrow ?screen-name)
         (u-table ?urow ?screen-name)
         (to-string ?screen-name :> ?sn)
         (c/count ?count))))

(defn distinct-timezones
  "How many timezones are there?"
  []
  (let [h-table (hbase-tap "twitter_accounts_copy" "row-id" "base" "time_zone")]
    (?<- (stdout) [?count] (h-table _ ?timezone)
         (to-string ?timezone :> ?tz)
         (c/distinct-count ?tz :> ?count))))
;; 139

(defn people-timezone-count
  "How many people from each timezone are there?"
  []
  (let [a-table (hbase-tap "twitter_accounts_copy" "row-id" "base" "time_zone")]
    (?<- (stdout) [?tz ?count] (a-table _ ?timezone)
         (to-string ?timezone :> ?tz)
         (:sort ?tz)
         (c/count ?count))))
