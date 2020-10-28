(ns datomic-tx-metrics.core
  (:require
    [aleph.http :as http]
    [bidi.ring :as bidi]
    [clojure.string :as string]
    [environ.core :refer [env]]
    [prometheus.alpha :as prom]
    [taoensso.timbre :as log])
  (:import [io.prometheus.client CollectorRegistry]
           [io.prometheus.client.hotspot StandardExports MemoryPoolsExports
                                         GarbageCollectorExports ThreadExports
                                         ClassLoadingExports VersionInfoExports]))

;; ---- Metrics ----------------------------------------------------------------

(prom/defgauge alarms
  "Number of alarms/problems that have occurred distinguished by their kind."
  {:namespace "datomic"}
  "service_name"
  "kind")

(prom/defgauge available-ram-bytes
  "Unused RAM on transactor in bytes."
  {:namespace "datomic"}
  "service_name")

(prom/defgauge object-cache-size
  "Number of segments in the Datomic object cache."
  {:namespace "datomic"}
  "service_name")

(prom/defcounter object-cache-count-total
  "Number of requests to the Datomic object cache"
  {:namespace "datomic"}
  "service_name")

(prom/defcounter object-cache-sum-total
  "Number of requests that hit the Datomic object cache"
  {:namespace "datomic"}
  "service_name")

;; Current number of connected peer, so gauge
(prom/defgauge remote-peers
  "Number of remote peers connected."
  {:namespace "datomic"}
  "service_name")

(prom/defcounter metric-reports-count-total
  "Number of metric reports reported"
  {:namespace "datomic"}
  "service_name")

(prom/defcounter metric-reports-sum-total
  "Number of metric reports reported successfully"
  {:namespace "datomic"}
  "service_name")

(prom/defcounter transacted-datoms-count-total
  "Number of transacted datoms."
  {:namespace "datomic"}
  "service_name")

(prom/defcounter transacted-datoms-sum-total
  "Total number of transactions."
  {:namespace "datomic"}
  "service_name")

(prom/defcounter transaction-batch-count-total
  "Number of transactions written to the log as reported by `TransactionBatch`."
  {:namespace "datomic"}
  "service_name")

(prom/defcounter transaction-batch-sum-total
  "Number of transaction batches written to the log"
  {:namespace "datomic"}
  "service_name")

(prom/defcounter transaction-bytes-count-total
  "Number of transactions written to the log as reported by `TransactionBytes`."
  {:namespace "datomic"}
  "service_name")

(prom/defcounter transaction-bytes-sum-total
  "Total volume of transaction data to log, peers in bytes."
  {:namespace "datomic"}
  "service_name")

(prom/defcounter transacted-seconds-count-total
  "Number of transactions written to the log as reported by `TransactionMsec`."
  {:namespace "datomic"}
  "service_name")

(prom/defcounter transactions-seconds-sum-total
  "Total time of transactions in seconds."
  {:namespace "datomic"}
  "service_name")

(prom/defcounter db-add-fulltext-seconds-count-total
  "Total number transactions adding fulltext indices."
  {:namespace "datomic"}
  "service_name")

(prom/defcounter db-add-fulltext-seconds-sum-total
  "Total time of transactions spent to add fulltext in seconds."
  {:namespace "datomic"}
  "service_name")

(prom/defcounter log-write-sec-count-total
  "Total number of writes to the log"
  {:namespace "datomic"}
  "service_name")

(prom/defcounter log-write-sec-sum-total
  "Total seconds spend writing to the log"
  {:namespace "datomic"}
  "service_name")

;; Gauge as it can go up and down
(prom/defgauge datoms
  "Number of unique datoms in the index."
  {:namespace "datomic"}
  "service_name")

;; Gauge as it can go up and down
(prom/defgauge index-datoms
  "Number of datoms stored by the index, all sorts."
  {:namespace "datomic"}
  "service_name")

;; Gauge as it can go up and down
(prom/defgauge index-segments
  "Number of segments in the index."
  {:namespace "datomic"}
  "service_name")

(prom/defcounter index-writes-count-total
  "Number of times indexes were written"
  {:namespace "datomic"}
  "service_name")

(prom/defcounter index-writes-sum-total
  "Number of indexes written"
  {:namespace "datomic"}
  "service_name")

(prom/defcounter index-writes-seconds-count-total
  "Number of indexes written"
  {:namespace "datomic"}
  "service_name")

(prom/defcounter index-writes-seconds-sum-total
  "Time per index write in seconds."
  {:namespace "datomic"}
  "service_name")

;; This is a single value at the end of a job, so gauge
(prom/defgauge create-entire-index-seconds
  "Time to create index in seconds, reported at end of indexing job."
  {:namespace "datomic"}
  "service_name")

;; This is a single value at the end of a job, so gauge
(prom/defgauge create-fulltext-index-seconds
  "Time to create fulltext portion of index in seconds."
  {:namespace "datomic"}
  "service_name")

;; This is a single value of currently consumed memory, so gauge
(prom/defgauge memory-index-consumed-bytes
  "RAM consumed by memory index in bytes."
  {:namespace "datomic"}
  "service_name")

;; Not documented
(prom/defgauge memory-index-fill-seconds
  "Estimate of the time to fill the memory index in seconds, given the current write load."
  {:namespace "datomic"}
  "service_name")

(prom/defcounter storage-put-bytes-count-total
  "Total number of storage write operations."
  {:namespace "datomic"}
  "service_name")

(prom/defcounter storage-put-bytes-sum-total
  "Total number of bytes written to storage."
  {:namespace "datomic"}
  "service_name")

(prom/defcounter storage-put-seconds-count-total
  "Total number of storage write operations."
  {:namespace "datomic"}
  "service_name")

(prom/defcounter storage-put-seconds-sum-total
  "Total duration of storage write operations."
  {:namespace "datomic"}
  "service_name")

(prom/defcounter storage-get-bytes-count-total
  "Total number of storage read operations."
  {:namespace "datomic"}
  "service_name")

(prom/defcounter storage-get-bytes-sum-total
  "Total number of bytes read from storage."
  {:namespace "datomic"}
  "service_name")

(prom/defcounter storage-get-seconds-count-total
  "Total number of storage read operations."
  {:namespace "datomic"}
  "service_name")

(prom/defcounter storage-get-seconds-sum-total
  "Total duration of storage read operations."
  {:namespace "datomic"}
  "service_name")

(prom/defcounter storage-backoff-count-total
  "Total number of storage backoff operations."
  {:namespace "datomic"}
  "service_name")

(prom/defcounter storage-backoff-seconds-sum-total
  "Total duration of storage backoff operations."
  {:namespace "datomic"}
  "service_name")

(prom/defcounter garbage-segments-count-total
  "Total number of times garbage segments were created."
  {:namespace "datomic"}
  "service_name")

(prom/defcounter garbage-segments-sum-total
  "Total garbage segments created."
  {:namespace "datomic"}
  "service_name")

(prom/defcounter heartbeat-seconds-count-total
  "Total number of heartbeats written by active transactor."
  {:namespace "datomic"}
  "service_name")

(prom/defcounter heartbeat-seconds-seconds-sum-total
  "Total duration of heartbeat interval."
  {:namespace "datomic"}
  "service_name")

;; Missing transactor stats
(prom/defgauge cluster-creation-seconds
  "time to create a \"file system\" in the storage"
  {:namespace "datomic"}
  "service_name")

(prom/defgauge fulltext-segments
  "total number of fulltext segments in the index, per index job"
  {:namespace "datomic"}
  "service_name")

(prom/defcounter log-ingest-bytes-count-total
  "number of times logs were ingested"
  {:namespace "datomic"}
  "service_name")

(prom/defcounter log-ingest-bytes-sum-total
  "total number of bytes ingested as logs"
  {:namespace "datomic"}
  "service_name")

(prom/defcounter log-ingest-seconds-count-total
  "number of times logs were ingested"
  {:namespace "datomic"}
  "service_name")

(prom/defcounter log-ingest-seconds-sum-total
  "total time spent writing to storage in seconds."
  {:namespace "datomic"}
  "service_name")

(prom/defcounter memcached-put-seconds-count-total
  "Number of samples of memcached put requests"
  {:namespace "datomic"}
  "service_name")

(prom/defcounter memcached-put-seconds-sum-total
  "Total duration of samples of memcached put requests"
  {:namespace "datomic"}
  "service_name")

(prom/defcounter memcached-put-failed-seconds-count-total
  "Number of failed samples of memcached put requests"
  {:namespace "datomic"}
  "service_name")

(prom/defcounter memcached-put-failed-seconds-sum-total
  "Total duration of failed samples of memcached put requests"
  {:namespace "datomic"}
  "service_name")

(prom/defcounter valcache-put-seconds-count-total
  "Number of samples of valcache put requests"
  {:namespace "datomic"}
  "service_name")

(prom/defcounter valcache-put-seconds-sum-total
  "Total duration of samples of valcache put requests"
  {:namespace "datomic"}
  "service_name")

(prom/defcounter valcache-put-failed-seconds-count-total
  "Number of failed samples of valcache put requests"
  {:namespace "datomic"}
  "service_name")

(prom/defcounter valcache-put-failed-seconds-sum-total
  "Total duration of failed samples of valcache put requests"
  {:namespace "datomic"}
  "service_name")

(prom/defcounter memcache-count-total
  "Times that the memcached was called."
  {:namespace "datomic"}
  "service_name")

(prom/defcounter memcached-sum-total
  "Times that the memcached was hit."
  {:namespace "datomic"}
  "service_name")

(prom/defcounter valcache-count-total
  "Times that the valcache was called."
  {:namespace "datomic"}
  "service_name")

(prom/defcounter valcache-sum-total
  "Times that the valcache was hit."
  {:namespace "datomic"}
  "service_name")

(prom/defgauge peer-accept-new-seconds
  "time for the peer to accept a new index."
  {:namespace "datomic"}
  "service_name")

(def ^:private metrics-registry
  (doto (CollectorRegistry. true)
    (.register (StandardExports.))
    (.register (MemoryPoolsExports.))
    (.register (GarbageCollectorExports.))
    (.register (ThreadExports.))
    (.register (ClassLoadingExports.))
    (.register (VersionInfoExports.))
    (.register alarms)
    (.register available-ram-bytes)
    (.register object-cache-size)
    (.register object-cache-count-total)
    (.register object-cache-sum-total)
    (.register remote-peers)
    (.register metric-reports-count-total)
    (.register metric-reports-sum-total)
    (.register transacted-datoms-count-total)
    (.register transacted-datoms-sum-total)
    (.register transaction-batch-count-total)
    (.register transaction-batch-sum-total)
    (.register transaction-bytes-count-total)
    (.register transaction-bytes-sum-total)
    (.register transacted-seconds-count-total)
    (.register transactions-seconds-sum-total)
    (.register db-add-fulltext-seconds-count-total)
    (.register db-add-fulltext-seconds-sum-total)
    (.register log-write-sec-count-total)
    (.register log-write-sec-sum-total)
    (.register datoms)
    (.register index-datoms)
    (.register index-segments)
    (.register index-writes-count-total)
    (.register index-writes-sum-total)
    (.register index-writes-seconds-count-total)
    (.register index-writes-seconds-sum-total)
    (.register create-entire-index-seconds)
    (.register create-fulltext-index-seconds)
    (.register memory-index-consumed-bytes)
    (.register memory-index-fill-seconds)
    (.register storage-put-bytes-count-total)
    (.register storage-put-bytes-sum-total)
    (.register storage-put-seconds-count-total)
    (.register storage-put-seconds-sum-total)
    (.register storage-get-bytes-count-total)
    (.register storage-get-bytes-sum-total)
    (.register storage-get-seconds-count-total)
    (.register storage-get-seconds-sum-total)
    (.register storage-backoff-count-total)
    (.register storage-backoff-seconds-sum-total)
    (.register garbage-segments-count-total)
    (.register garbage-segments-sum-total)
    (.register heartbeat-seconds-count-total)
    (.register heartbeat-seconds-seconds-sum-total)
    (.register cluster-creation-seconds)
    (.register fulltext-segments)
    (.register log-ingest-bytes-count-total)
    (.register log-ingest-bytes-sum-total)
    (.register log-ingest-seconds-count-total)
    (.register log-ingest-seconds-sum-total)
    (.register memcached-put-seconds-count-total)
    (.register memcached-put-seconds-sum-total)
    (.register memcached-put-failed-seconds-count-total)
    (.register memcached-put-failed-seconds-sum-total)
    (.register valcache-put-seconds-count-total)
    (.register valcache-put-seconds-sum-total)
    (.register valcache-put-failed-seconds-count-total)
    (.register valcache-put-failed-seconds-sum-total)
    (.register memcache-count-total)
    (.register memcached-sum-total)
    (.register valcache-count-total)
    (.register valcache-sum-total)
    (.register peer-accept-new-seconds)))

;; ---- Server -----------------------------------------------------------------

(defn- msec-to-sec
  "Converts a `value` given in msec to sec."
  [value]
  (/ (double value) 1000))


(defn- mb-to-bytes
  "Converts a `value` given in MB to B."
  [value]
  (* (double value) 1000000))

;; ---- Server -----------------------------------------------------------------
(def server (atom nil))

(defn- wrap-not-found
  "Middleware which returns a 404 response if no downstream handler can be
   found processing a request. Otherwise forwards the request to the found
   handler as well as its response to the caller."
  [handler]
  (fn [req]
    (if-let [resp (handler req)]
      resp
      {:status 404
       :header {:content-type "text/plain"}
       :body "Not Found"})))


(defn- health-handler
  "Health handler returning a 200 response code with 'OK' as a response body."
  [_]
  {:status 200 :body "OK"})


(defn- metrics-handler
  "Metrics handler returning the transactor and JVM metrics of a transactor."
  [_]
  (prom/dump-metrics metrics-registry))


(def ^:private routes
  "Defines the routes for the web server."
  ["/"
   [["health" {:get health-handler}]
    ["metrics" {:get metrics-handler}]]])

(defn- routing
  "Creates a ring handler for routing requests to the appropriate sub-handler
   based on `routes`."
  [routes]
  (-> (bidi/make-handler routes)
      (wrap-not-found)))


(defn- start-metrics-server
  "Starts the web server that can be used to scrape transactor + JVM metrics."
  []
  (let [metrics-port (Integer/parseInt (or (:metrics-port env) "11509"))]
    (log/info "Starting metrics server on port " metrics-port)
    (http/start-server (routing routes) {:port metrics-port})))

;; ---- Callback ---------------------------------------------------------------

(def service-name (:service-name env))

(defn tx-metrics-callback-handler
  "Called by Datomic transactor transferring its metrics."
  [tx-metrics]
  ;; If no server was running, start one now.
  (swap! server (fn [server] (or server (start-metrics-server))))

  (if-let [{:keys [sum]} (:AlarmIndexingJobFailed tx-metrics)]
    (prom/set! alarms service-name "index-job-failed" sum)
    (prom/set! alarms service-name "index-job-failed" 0))

  (when-let [{:keys [sum]} (:AlarmBackPressure tx-metrics)]
    (prom/set! alarms service-name "back-pressure" sum)
    (prom/set! alarms service-name "back-pressure" 0))

  (when-let [{:keys [sum]} (:AlarmUnhandledException tx-metrics)]
    (prom/set! alarms service-name "unhandled-exception" sum)
    (prom/set! alarms service-name "unhandled-exception" 0))

  (->> (keys tx-metrics)
       (filter
        (fn [key]
          (and (string/starts-with? (name key) "Alarm")
               (not= key :Alarm)
               (not= key :AlarmIndexingJobFailed)
               (not= key :AlarmBackPressure)
               (not= key :AlarmUnhandledException))))
       (reduce
        (fn [count {:keys [sum]}]
          (+ count sum))
        0)
       (prom/set! alarms service-name "other"))

  (when-let [mb (:AvailableMB tx-metrics)]
    (prom/set! available-ram-bytes service-name (mb-to-bytes mb)))

  (when-let [size (:ObjectCacheCount tx-metrics)]
    (prom/set! object-cache-size service-name size))

  (when-let [{:keys [sum]} (:RemotePeers tx-metrics)]
    (prom/set! remote-peers service-name sum))

  (when-let [{:keys [sum count]} (:MetricsReport tx-metrics)]
    (prom/inc! metric-reports-count-total service-name count)
    (prom/inc! metric-reports-count-total service-name sum))

  (when-let [{:keys [sum count]} (:TransactionDatoms tx-metrics)]
    (prom/inc! transacted-datoms-count-total service-name count)
    (prom/inc! transacted-datoms-sum-total   service-name sum))

  (when-let [{:keys [sum count]} (:TransactionBatch tx-metrics)]
    (prom/inc! transaction-batch-count-total service-name count)
    (prom/inc! transaction-batch-sum-total   service-name sum))

  (when-let [{:keys [sum count]} (:TransactionBytes tx-metrics)]
    (prom/inc! transaction-bytes-count-total service-name count)
    (prom/inc! transaction-bytes-sum-total   service-name sum))

  (when-let [{:keys [sum count]} (:TransactionMsec tx-metrics)]
    (prom/inc! transacted-seconds-count-total service-name count)
    (prom/inc! transactions-seconds-sum-total service-name (msec-to-sec sum)))

  (when-let [{:keys [sum count]} (:DBAddFulltextMsec tx-metrics)]
    (prom/inc! db-add-fulltext-seconds-count-total service-name count)
    (prom/inc! db-add-fulltext-seconds-sum-total   service-name (msec-to-sec sum)))

  (when-let [{:keys [sum count]} (:LogWriteMsec tx-metrics)]
    (prom/inc! log-write-sec-count-total service-name count)
    (prom/inc! log-write-sec-sum-total   service-name (msec-to-sec sum)))

  (if-let [{:keys [sum]} (:Datoms tx-metrics)]
    (prom/set! datoms service-name sum)
    (prom/clear! datoms))

  ; TODO: check if resetting this is actually what resembles the transactor state
  (if-let [{:keys [sum]} (:IndexDatoms tx-metrics)]
    (prom/set! index-datoms service-name sum)
    (prom/set! index-datoms service-name 0))

  ; TODO: check if resetting this is actually what resembles the transactor state
  (if-let [{:keys [sum]} (:IndexSegments tx-metrics)]
    (prom/set! index-segments service-name sum)
    (prom/set! index-segments service-name 0))

  (when-let [{:keys [sum count]} (:IndexWrites tx-metrics)]
    (prom/set! index-writes-count-total service-name count)
    (prom/set! index-writes-count-total service-name sum))

  (when-let [{:keys [sum count]} (:IndexWriteMsec tx-metrics)]
    (prom/inc! index-writes-count-total service-name count)
    (prom/inc! index-writes-sum-total   service-name (msec-to-sec sum)))

  (if-let [{:keys [sum]} (:CreateEntireIndexMsec tx-metrics)]
    (prom/set! create-entire-index-seconds service-name (msec-to-sec sum))
    (prom/set! create-entire-index-seconds service-name 0))

  (if-let [{:keys [sum]} (:CreateFulltextIndexMsec tx-metrics)]
    (prom/set! create-fulltext-index-seconds service-name (msec-to-sec sum))
    (prom/set! create-fulltext-index-seconds service-name 0))

  (when-let [{:keys [sum]} (:MemoryIndexMB tx-metrics)]
    (prom/set! memory-index-consumed-bytes service-name (mb-to-bytes sum)))

  (if-let [{:keys [sum]} (:MemoryIndexFillMsec tx-metrics)]
    (prom/set! memory-index-fill-seconds service-name (msec-to-sec sum))
    (prom/set! memory-index-fill-seconds service-name 0))

  (when-let [{:keys [sum count]} (:StoragePutBytes tx-metrics)]
    (prom/inc! storage-put-bytes-count-total service-name count)
    (prom/inc! storage-put-bytes-sum-total   service-name sum))

  (when-let [{:keys [sum count]} (:StoragePutMsec tx-metrics)]
    (prom/inc! storage-put-seconds-count-total service-name count)
    (prom/inc! storage-put-seconds-sum-total   service-name (msec-to-sec sum)))

  (when-let [{:keys [sum count]} (:StorageGetBytes tx-metrics)]
    (prom/inc! storage-get-bytes-count-total service-name count)
    (prom/inc! storage-get-bytes-sum-total   service-name sum))

  (when-let [{:keys [sum count]} (:StorageGetMsec tx-metrics)]
    (prom/inc! storage-get-seconds-count-total service-name count)
    (prom/inc! storage-get-seconds-sum-total   service-name (msec-to-sec sum)))

  (when-let [{:keys [sum count]} (:StorageBackoff tx-metrics)]
    (prom/inc! storage-backoff-count-total       service-name count)
    (prom/inc! storage-backoff-seconds-sum-total service-name (msec-to-sec sum)))

  (when-let [{:keys [sum count]} (:ObjectCache tx-metrics)]
    (prom/inc! object-cache-count-total service-name count)
    (prom/inc! object-cache-sum-total   service-name sum))

  (when-let [{:keys [sum count]} (:GarbageSegments tx-metrics)]
    (prom/inc! garbage-segments-sum-total service-name count)
    (prom/inc! garbage-segments-sum-total service-name sum))

  (when-let [{:keys [sum count]} (:HeartbeatMsec tx-metrics)]
    (prom/inc! heartbeat-seconds-count-total       service-name count)
    (prom/set! heartbeat-seconds-seconds-sum-total service-name (msec-to-sec sum)))

  ;; Missing transactor stats
  (if-let [{:keys [sum]} (:ClusterCreateFS tx-metrics)]
    (prom/set! cluster-creation-seconds service-name (msec-to-sec sum))
    (prom/set! cluster-creation-seconds service-name 0))

  (if-let [{:keys [sum]} (:FulltextSegments tx-metrics)]
    (prom/set! fulltext-segments   service-name sum)
    (prom/clear! fulltext-segments))

  (when-let [{:keys [sum count]} (:LogIngestBytes tx-metrics)]
    (prom/inc! log-ingest-bytes-count-total service-name count)
    (prom/inc! log-ingest-bytes-sum-total   service-name sum))

  (when-let [{:keys [sum count]} (:LogIngestMsec tx-metrics)]
    (prom/inc! log-ingest-bytes-count-total service-name count)
    (prom/inc! log-ingest-bytes-sum-total   service-name (msec-to-sec sum)))

  (when-let [{:keys [sum count]} (:Memcached tx-metrics)]
    (prom/inc! memcache-count-total service-name count)
    (prom/inc! memcached-sum-total  service-name sum))

  (when-let [{:keys [sum count]} (:MemcachedPutMsec tx-metrics)]
    (prom/inc! memcached-put-seconds-count-total service-name count)
    (prom/inc! memcached-put-seconds-sum-total   service-name sum))

  (when-let [{:keys [sum count]} (:MemcachedPutFailedMsec tx-metrics)]
    (prom/inc! memcached-put-failed-seconds-count-total service-name count)
    (prom/inc! memcached-put-failed-seconds-sum-total   service-name (msec-to-sec sum)))

  (when-let [{:keys [sum count]} (:Valcache tx-metrics)]
    (prom/inc! valcache-count-total service-name count)
    (prom/inc! valcache-sum-total   service-name sum))

  (when-let [{:keys [sum count]} (:ValcachePutMsec tx-metrics)]
    (prom/inc! valcache-put-seconds-count-total service-name count)
    (prom/inc! valcache-put-seconds-sum-total   service-name sum))

  (when-let [{:keys [sum count]} (:ValcachePutFailedMsec tx-metrics)]
    (prom/inc! valcache-put-failed-seconds-count-total service-name count)
    (prom/inc! valcache-put-failed-seconds-sum-total   service-name (msec-to-sec sum)))

  ;; Missing peer stats
  (if-let [{:keys [sum]} (:PeerAcceptNewMsec tx-metrics)]
    (prom/set! peer-accept-new-seconds service-name (msec-to-sec sum))
    (prom/set! peer-accept-new-seconds service-name 0)))
