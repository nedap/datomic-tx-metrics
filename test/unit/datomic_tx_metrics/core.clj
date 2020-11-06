(ns unit.datomic-tx-metrics.core
  (:require
   [clojure.test :refer :all]
   [datomic-tx-metrics.core :as sut]))


(deftest tx-metrics-callback-handler
  (alter-var-root #'sut/service-name (constantly "datomic"))
  (alter-var-root #'sut/start-metrics-server (constantly (fn [] ::server)))

  (are [input] (do (sut/tx-metrics-callback-handler input)
                   true)
    {:Alarm {:sum 1 :count 1}}
    {:AlarmIndexingJobFailed {:sum 1 :count 1}}
    {:AlarmBackPressure {:sum 1 :count 1}}
    {:AlarmUnhandledException {:sum 1 :count 1}}
    {:AlarmHeartbeatFailed {:sum 1 :count 1}}
    {:AvailableMB 12}
    {:ObjectCacheCount 12}
    {:RemotePeers {:sum 1 :count 1}}
    {:MetricsReport {:sum 1 :count 1}}
    {:TransactionDatoms {:sum 1 :count 1}}
    {:TransactionBatch {:sum 1 :count 1}}
    {:TransactionBytes {:sum 1 :count 1}}
    {:TransactionMsec {:sum 1 :count 1}}
    {:DBAddFulltextMsec {:sum 1 :count 1}}
    {:LogWriteMsec {:sum 1 :count 1}}
    {:Datoms {:sum 1 :count 1}}
    {:IndexDatoms {:sum 1 :count 1}}
    {:IndexSegments {:sum 1 :count 1}}
    {:IndexWrites {:sum 1 :count 1}}
    {:IndexWriteMsec {:sum 1 :count 1}}
    {:CreateEntireIndexMsec {:sum 1 :count 1}}
    {:CreateFulltextIndexMsec {:sum 1 :count 1}}
    {:MemoryIndexMB {:sum 1 :count 1}}
    {:MemoryIndexFillMsec {:sum 1 :count 1}}
    {:StoragePutBytes {:sum 1 :count 1}}
    {:StoragePutMsec {:sum 1 :count 1}}
    {:StorageGetBytes {:sum 1 :count 1}}
    {:StorageGetMsec {:sum 1 :count 1}}
    {:StorageBackoff {:sum 1 :count 1}}
    {:ObjectCache {:sum 1 :count 1}}
    {:GarbageSegments {:sum 1 :count 1}}
    {:HeartbeatMsec {:sum 1 :count 1}}
    {:ClusterCreateFS {:sum 1 :count 1}}
    {:FulltextSegments {:sum 1 :count 1}}
    {:LogIngestBytes {:sum 1 :count 1}}
    {:LogIngestMsec {:sum 1 :count 1}}
    {:Memcached {:sum 1 :count 1}}
    {:MemcachedPutMsec {:sum 1 :count 1}}
    {:MemcachedPutFailedMsec {:sum 1 :count 1}}
    {:Valcache {:sum 1 :count 1}}
    {:ValcachePutMsec {:sum 1 :count 1}}
    {:ValcachePutFailedMsec {:sum 1 :count 1}}
    {:PeerAcceptNewMsec {:sum 1 :count 1}}))
