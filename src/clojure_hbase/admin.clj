(ns clojure-hbase.admin
  (:refer-clojure :rename {get map-get} :exclude [flush])
  (:use clojure-hbase.core
        clojure-hbase.internal)
  (:import [org.apache.hadoop.hbase HBaseConfiguration HConstants
            HTableDescriptor HColumnDescriptor]
           [org.apache.hadoop.hbase.client HBaseAdmin]
           [org.apache.hadoop.hbase.util Bytes]
           [org.apache.hadoop.hbase.io.hfile Compression]))

(def ^{:tag HBaseAdmin :dynamic true} *admin*
  (atom nil))

(defn hbase-admin
  "When called with no argument, creates a new HBaseAdmin from a default config and sets it to the *admin* atom.
   When called with a config-obj, gets a HBaseAdmin from the hbase cluster represented by the given
   HBaseConfiguration object."
  (^HBaseAdmin []
   (when-not @*admin*
     (swap! *admin* #(or % (HBaseAdmin. (HBaseConfiguration/create)))))
   @*admin*)
  ([config-obj] (HBaseAdmin. config-obj)))

(defn set-admin-config
  "Resets the *admin* atom to a new HBaseAdmin object that uses the
   given HBaseConfiguration.

   Example: (set-admin-config
              (make-config
                {\"hbase.zookeeper.dns.interface\" \"lo\"
                 :hbase.zookeeper.quorum \"127.0.0.1\"})"
  [^HBaseConfiguration config-obj]
  (reset! *admin* (HBaseAdmin. config-obj)))

;;
;; HColumnDescriptor
;;

;; This maps each get command to its number of arguments, for helping us
;; partition the command sequence.
(def ^{:private true} column-desc-argnums
  {:block-cache-enabled      1   ;; :block-cache-enabled <boolean>
   :block-size               1   ;; :block-size <int>
   :bloom-filter-type        1   ;; :bloom-filter <StoreFile.BloomType>
   :compression-type         1   ;; :compression-type <Compression.Algorithm>
   :in-memory                1   ;; :in-memory <boolean>
   :max-versions             1   ;; :max-versions <int>
   :time-to-live             1}) ;; :time-to-live <int>

(defn column-descriptor
  [family-name & args]
  (let [specs (partition-query args column-desc-argnums)
        ^HColumnDescriptor cd (HColumnDescriptor. (to-bytes family-name))]
    (doseq [spec specs]
      (condp = (first spec)
          :block-cache-enabled      (.setBlockCacheEnabled cd (second spec))
          :block-size               (.setBlocksize cd (second spec))
          :bloom-filter-type        (.setBloomFilterType cd (second spec))
          :compression-type         (.setCompressionType cd (second spec))
          :in-memory                (.setInMemory cd (second spec))
          :max-versions             (.setMaxVersions cd (second spec))
          :time-to-live             (.setTimeToLive cd (second spec))))
    cd))

;;
;; HTableDescriptor
;;

;; This maps each get command to its number of arguments, for helping us
;; partition the command sequence.
(def ^{:private true} table-desc-argnums
  {:max-file-size         1   ;; :max-file-size <long>
   :mem-store-flush-size  1   ;; :mem-store-flush-size <long>
   :read-only             1   ;; :read-only <boolean>
   :family                1}) ;; :family <HColumnDescriptor>

(defn table-descriptor
  [table-name & args]
  (let [specs (partition-query args table-desc-argnums)
        td (HTableDescriptor. (to-bytes table-name))]
    (doseq [spec specs]
      (condp = (first spec)
          :max-file-size         (.setMaxFileSize td (second spec))
          :mem-store-flush-size  (.setMemStoreFlushSize td (second spec))
          :read-only             (.setReadOnly td (second spec))
          :family                (.addFamily td (second spec))))
    td))


;;
;; HBaseAdmin
;;

(defn add-column-family
  ([table-name ^HColumnDescriptor column-descriptor] (add-column-family (hbase-admin) table-name column-descriptor))
  ([^HBaseAdmin admin table-name ^HColumnDescriptor column-descriptor]
   (.addColumn admin (to-bytes table-name) column-descriptor)))

(defn hbase-available?
  ([] (hbase-available? (HBaseConfiguration.)))
  ([config-obj] (HBaseAdmin/checkHBaseAvailable config-obj)))

(defn compact
  ([table-or-region-name] (compact (hbase-admin) table-or-region-name))
  ([admin table-or-region-name] (.compact admin (to-bytes table-or-region-name))))

(defn create-table
  ([table-descriptor] (create-table (hbase-admin) table-descriptor))
  ([admin table-descriptor] (.createTable admin table-descriptor)))

(defn create-table-async
  ([^HTableDescriptor table-descriptor split-keys] (create-table-async (hbase-admin) table-descriptor split-keys))
  ([^HBaseAdmin admin ^HTableDescriptor table-descriptor split-keys] (.createTableAsync admin table-descriptor split-keys)))

(defn delete-column-family
  ([table-name column-name] (delete-column-family (hbase-admin) table-name column-name))
  ([admin table-name column-name] (.deleteColumn admin (to-bytes table-name) (to-bytes column-name))))

(defn delete-table
  ([table-name] (delete-table (hbase-admin) table-name))
  ([admin table-name] (.deleteTable admin (to-bytes table-name))))

(defn disable-table
  ([table-name] (disable-table (hbase-admin) table-name))
  ([admin table-name] (.disableTable admin (to-bytes table-name))))

(defn enable-table
  ([table-name] (enable-table (hbase-admin) table-name))
  ([admin table-name] (.enableTable admin (to-bytes table-name))))

(defn flush
  ([table-or-region-name] (flush (hbase-admin) table-or-region-name))
  ([admin table-or-region-name] (.flush admin (to-bytes table-or-region-name))))

(defn cluster-status
  ([] (cluster-status (hbase-admin)))
  ([admin] (.getClusterStatus admin)))

(defn get-connection
  ([] (get-connection (hbase-admin)))
  ([admin] (.getConnection admin)))

(defn get-master
  ([] (get-master (hbase-admin)))
  ([admin] (.getMaster admin)))

(defn get-table-descriptor
  ([table-name] (get-table-descriptor (hbase-admin) table-name))
  ([admin table-name] (.getTableDescriptor admin (to-bytes table-name))))

(defn master-running?
  ([] (master-running? (hbase-admin)))
  ([admin] (.isMasterRunning admin)))

(defn table-available?
  ([table-name] (table-available? (hbase-admin) table-name))
  ([admin table-name] (.isTableAvailable admin (to-bytes table-name))))

(defn table-disabled?
  ([table-name] (table-disabled? (hbase-admin) table-name))
  ([admin table-name] (.isTableDisabled admin (to-bytes table-name))))

(defn table-enabled?
  ([table-name] (table-enabled? (hbase-admin) table-name))
  ([admin table-name] (.isTableEnabled admin (to-bytes table-name))))

(defn list-tables
  ([] (list-tables (hbase-admin)))
  ([admin] (seq (.listTables admin))))

(defn major-compact
  ([table-or-region-name] (major-compact (hbase-admin) table-or-region-name))
  ([admin table-or-region-name] (.majorCompact admin (to-bytes table-or-region-name))))

(defn modify-column-family
  ([table-name column-name ^HColumnDescriptor column-descriptor]
   (modify-column-family (hbase-admin) table-name column-name column-descriptor))
  ([admin table-name column-name ^HColumnDescriptor column-descriptor]
   (.modifyColumn admin (to-bytes table-name) (to-bytes column-name) column-descriptor)))

(defn modify-table
  ([table-name table-descriptor] (modify-table (hbase-admin) table-name table-descriptor))
  ([admin table-name table-descriptor] (.modifyTable admin (to-bytes table-name) table-descriptor)))

(defn shutdown
  ([] (shutdown (hbase-admin)))
  ([admin] (.shutdown admin)))

(defn split
  ([table-or-region-name] (.split (hbase-admin) table-or-region-name))
  ([admin table-or-region-name] (.split admin (to-bytes table-or-region-name))))

(defn table-exists?
  ([table-name] (table-exists? (hbase-admin) table-name))
  ([admin table-name] (.tableExists admin (to-bytes table-name))))
