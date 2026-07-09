(ns nd-db.protocol
  (:require [clojure.java.io :as io]
           [taoensso.nippy :as nippy])
  (:import [java.io
           ByteArrayInputStream ByteArrayOutputStream
           DataInputStream DataOutputStream]))

;; From data to nippy bytes and back
(let [bos (ByteArrayOutputStream.)
      dos (DataOutputStream. bos)]
  (nippy/freeze-to-out! dos {:a "b"})
  (nippy/thaw-from-in!
   (-> bos .toByteArray ByteArrayInputStream. DataInputStream.)))

;; we can save around 20% storage space, by reading directly from the
;; DataInput instead of converting to b64-encoded newlines.

;; Is it worth it, when we then no longer can just take 10 lines of a
;; file and get a partial database?

;; Short answer: No.

;; Long answer: There's more to it.
;; Reading from the DataInput is around 90 times faster (!) than reading
;; the b64 encoded string.

(def much-data (into [] (map str (range 1000000))))

;; This takes around 90-95ms:
(let [frozen (nippy/freeze-to-string much-data)]
  (time (let [res (nippy/thaw-from-string frozen)])))

;; whereas this takes around 1.1-1.2 ms:
(let [bos (ByteArrayOutputStream.)
      dos (DataOutputStream. bos)]
  (nippy/freeze-to-out! dos (into [] (map str (range 1000000))))
  (time (let [dis (-> bos .toByteArray ByteArrayInputStream. DataInputStream.)
              res (.readAllBytes dis)])))

;; That's a raw read speed increase of 75x !!

;; But we can't leverage that speed increase, without:
;; 1. Dropping the ability newline delimited goodieness, and
;; 2.

(comment
  "type chars:

  17: empty vector
  18: empty set
  19: empty map

  34: empty string

  25: non-empty list
  35: empty list

  53: empty byte-array

  56: symbol
  105: non-empty string
  106: keyword

  111: non-empty set
  112/33/30: non-empty map
  114/110/69/21: non-empty vector")
