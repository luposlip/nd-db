(ns nd-db.compress
  (:require [clojure.java.io :as io])
  (:import [java.io
            ByteArrayInputStream ByteArrayOutputStream
            BufferedInputStream BufferedOutputStream]
           [org.apache.commons.compress.archivers.zip
            ZipArchiveEntry ZipArchiveOutputStream ZipFile]
           [org.apache.commons.compress.archivers.tar
            TarArchiveEntry TarArchiveOutputStream TarFile]
           [org.apache.commons.compress.compressors
            CompressorInputStream CompressorStreamFactory]))

(defn compressed-input-stream ^CompressorInputStream [filename]
  (let [in ^BufferedInputStream (io/input-stream filename)]
    (.createCompressorInputStream (CompressorStreamFactory.) in)))

(defn zip-output-stream ^ZipArchiveOutputStream [filename]
  (let [out ^BufferedOutputStream (io/output-stream filename)]
    (ZipArchiveOutputStream. out)))

(defn tar-output-stream ^TarArchiveOutputStream [filename]
  (let [out ^BufferedOutputStream (io/output-stream filename)]
    (TarArchiveOutputStream. out)))

(defn write-zip-entry! [^ZipArchiveOutputStream zip-os ^"[B" bytes ^String entry-name]
  (let [ze (ZipArchiveEntry. entry-name)]
    (.setSize ze (count bytes))
    (with-open [is ^ByteArrayInputStream (ByteArrayInputStream. bytes)]
      (.putArchiveEntry zip-os ze)
      (io/copy is zip-os)
      (.closeArchiveEntry zip-os))))

(defn write-tar-entry! [^TarArchiveOutputStream tar-os ^"[B" bytes ^String entry-name]
  (let [te (TarArchiveEntry. entry-name)]
    (.setSize te (count bytes))
    (with-open [is ^ByteArrayInputStream (ByteArrayInputStream. bytes)]
      (.putArchiveEntry tar-os te)
      (io/copy is tar-os)
      (.closeArchiveEntry tar-os))))

(defn tar-file [^String filename]
  (TarFile. (io/file filename)))

(defn read-zip-entry! ^"[B" [^ZipFile zf ^String entry-name]
  (let [ze (.getEntry zf entry-name)
        baos (ByteArrayOutputStream.)]
    (when ze
      (io/copy (.getInputStream zf ze) baos)
      (.toByteArray ^ByteArrayOutputStream baos))))

(defn read-first-zip-entry! ^"[B" [^String zip-file]
  (let [zf (ZipFile. zip-file)
        bytes (read-zip-entry! zf (.getName ^ZipArchiveEntry (.nextElement (.getEntries zf))))]
    (.close zf)
    bytes))

(defn finish-and-close-zip-outputstream! [^ZipArchiveOutputStream zos]
  (doto zos
    (.finish)
    (.close)))

(defn finish-and-close-tar-outputstream! [^TarArchiveOutputStream zos]
  (doto zos
    (.finish)
    (.close)))
#_
(with-open [zos (zip-output-stream "filename.zip")]
  (write-zip-entry! zos (.getBytes "bytes-to-write") "zip-entry-name.txt")
  (.finish zos))
