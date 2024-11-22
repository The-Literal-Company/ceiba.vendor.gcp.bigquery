(ns ceiba.vendor.gcp.bigquery.client
  (:require [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clojure.walk :as walk])
  (:import (com.google.cloud RetryOption)
           (com.google.cloud.bigquery BigQuery BigQuery$DatasetDeleteOption BigQuery$DatasetListOption BigQuery$DatasetOption BigQuery$JobOption BigQuery$TableListOption BigQuery$TableOption BigQueryException BigQueryOptions ConnectionProperty CopyJobConfiguration InsertAllRequest InsertAllRequest$Builder InsertAllResponse JobInfo QueryJobConfiguration QueryParameterValue StandardSQLTypeName TableId)
           (java.sql Timestamp)
           (java.time Instant LocalDate ZonedDateTime)
           (java.time.format DateTimeFormatter)
           (java.time.temporal ChronoUnit)
           (java.util ArrayList HashMap)))

(defonce ^:dynamic *session-id* nil)

(defn ^BigQuery build
  [{project-id :gcp/project}]
  ;https://cloud.google.com/java/docs/reference/google-cloud-bigquery/latest/com.google.cloud.bigquery.BigQueryOptions.Builder
  (let [options-builder (BigQueryOptions/newBuilder)]
    (.setProjectId options-builder project-id)
    (.getService (.build options-builder))))

(defn list-datasets [^BigQuery bq]
  (let [opts (into-array BigQuery$DatasetListOption [])]
    (seq (.iterateAll (.listDatasets bq opts)))))

(defn get-dataset
  [^BigQuery bq dataset-id]
  (let [opts (into-array BigQuery$DatasetOption [])]
    (.getDataset bq ^String dataset-id ^BigQuery$DatasetOption/1 opts)))

(defn delete-dataset
  "will delete contents if any"
  [^BigQuery bq dataset-id]
  (let [opts (into-array BigQuery$DatasetDeleteOption [(BigQuery$DatasetDeleteOption/deleteContents)])]
    (.delete bq ^String dataset-id ^BigQuery$DatasetDeleteOption/1 opts)))

(defn list-tables
  [^BigQuery bq dataset-id]
  (let [opts (into-array BigQuery$TableListOption [])
        iter (.listTables bq ^String dataset-id ^BigQuery$TableListOption/1 opts)]
    (seq (.iterateAll iter))))

(defn get-table
  [^BigQuery bq dataset-id table-id]
  (let [table-opts (into-array BigQuery$TableOption [])
        table-id (TableId/of dataset-id table-id)]
    (.getTable bq table-id table-opts)))

(defn delete-table
  [^BigQuery bq dataset-id table-id]
  (let [table-id (TableId/of dataset-id table-id)]
    (.delete bq table-id)))

#!=============================================================================
#!
#! InsertAllRequest
#!

(defn- errors-by-row [rows insert-errors]
  (into {}
        (map
          (fn [[n [error]]] ;seems like this list is always singular
            [n {:msg           (.getMessage error)
                :reason        (.getReason error)
                :incident-item (nth rows n)
                :error         error}]))
        insert-errors))

(defn- serialize-row [row] (into {} (walk/stringify-keys row)))

(defn insert-rows
  "produces errors of shape:
    {:cause \"Error inserting rows\"
     :data {:errors-by-row {0 {:errors [{:msg \"no such field :foo'\"..} ...]
                               :incident-item {:foo 42}}}}"
  [bq dataset-name table-id rows]
  (let [^TableId tableId (TableId/of dataset-name table-id)
        request-builder (InsertAllRequest/newBuilder tableId)
        _ (doseq [row rows]
            (.addRow ^InsertAllRequest$Builder request-builder ^HashMap (serialize-row row)))
        ^InsertAllResponse response (.insertAll bq (.build request-builder))]
    (when (.hasErrors response)
      (let [errors-by-row (errors-by-row rows (.getInsertErrors response))]
        (throw (ex-info "Error inserting rows" {:errors-by-row errors-by-row}))))))

#!================================================================
#!
#! Query helpers...must use ~*dataset qualified tables*~ within sql
#!

(defn- inst-from-double [val]
  (let [epoch-seconds (double val)
        seconds (long epoch-seconds)
        nanos (Math/round (* (- epoch-seconds seconds) 1e9))]
    (.truncatedTo (Instant/ofEpochSecond seconds nanos) ChronoUnit/MILLIS)))

(defn- ^Instant from-double-str [s]
  (inst-from-double (Double/parseDouble s)))

(defn- parse-field-value [col-type val]
  (case col-type
    "STRING" (if (string? val)
               val
               (mapv #(.getStringValue %) val)) ;; STRING :repeated shows up here w/ list
    "TIMESTAMP" (from-double-str val)
    "FLOAT" (Double/parseDouble val)
    "INTEGER" (Integer/parseInt val)
    "DATE" (LocalDate/parse val (DateTimeFormatter/ofPattern "yyyy-MM-dd"))
    "NUMERIC" (edn/read-string val)
    "BOOLEAN" (edn/read-string val)
    "JSON" (json/read-str val :key-fn keyword)
    (throw (ex-info "Cannot parse unknown type" {:type col-type :val val}))))

(defn- parse-field
  [{:keys [col-type col-name]} val]
  [(keyword col-name) (some-> val ((partial parse-field-value col-type)))])

(defn- parse-row [cols row]
  (into {} (map parse-field cols row)))

(defn- param-type [value]
  (cond
    (string? value) StandardSQLTypeName/STRING
    (integer? value) StandardSQLTypeName/INT64
    (float? value) StandardSQLTypeName/FLOAT64
    (double? value) StandardSQLTypeName/FLOAT64
    (instance? ZonedDateTime value) StandardSQLTypeName/TIMESTAMP
    (instance? LocalDate value) StandardSQLTypeName/DATE
    :else
    (throw (IllegalArgumentException. ^String (str "Unsupported parameter type: " (pr-str value))))))

(defn- param-value [value]
  (cond
    (string? value) (str value)
    (integer? value) (str value)
    (float? value) (str value)
    (double? value) (str value)
    (instance? ZonedDateTime value) (Timestamp/from (.toInstant ^ZonedDateTime value))
    (and (vector? value)
         (every? string? value)) (into-array String value)
    :else
    (throw (IllegalArgumentException. ^String (str "Unsupported parameter type: " (type value))))))

(defn- query-param [val]
  (-> (QueryParameterValue/newBuilder)
      (.setType (param-type val))
      (.setValue (param-value val))
      .build))

(defn- set-query-params!
  [query-config-builder params]
  (when params
    (if (map? params)
      (do
        (.setParameterMode query-config-builder "NAMED")
        (doseq [[k v] params]
          (when (some? v)
            (.addNamedParameter query-config-builder (name k) (query-param val)))))
      (do
        (.setParameterMode query-config-builder "POSITIONAL")
        (.setPositionalParameters query-config-builder (map query-param params))))))

(defn query-job
  ([bq sql]
   (query-job bq sql nil))
  ([bq sql params]
   (let [query-config-builder (QueryJobConfiguration/newBuilder sql)
         _ (.setUseLegacySql query-config-builder false)
         _ (when params
             (set-query-params! query-config-builder params))
         job-options []]
     (when *session-id*
       (let [prop-list (ArrayList.)]
         (.add prop-list (ConnectionProperty/of "session_id" *session-id*))
         (.setConnectionProperties query-config-builder prop-list)))
     (.query bq (.build query-config-builder) (into-array BigQuery$JobOption job-options)))))

(defn- result->clj
  [table-result]
  (when-let [schema (.getSchema table-result)]
    (let [cols (map (fn [f]
                      {:col-type (.name (.getType f))
                       :col-name (.getName f)})
                    (.getFields schema))
          rows (map (fn [row]
                      (map #(.getValue %) row))
                    (into [] (.iterateAll table-result)))]
      (mapv (partial parse-row cols) rows))))

(defn- error->map
  [bq-error]
  {:msg        (.getMessage bq-error)
   :location   (.getLocation bq-error)
   :reason     (.getReason bq-error)
   :debug-info (.getDebugInfo bq-error)})

(defn query
  ([bq sql]
   (query bq sql nil))
  ([bq sql params]
   (try
     (result->clj (query-job bq sql params))
     (catch BigQueryException bqe
       (if-let [errors (not-empty (.getErrors bqe))]
         (throw (ex-info "BigQueryErrors" {:errors (map error->map errors)}))
         (throw bqe))))))

(defn copy-table "nil or err"
  ([bq source-dataset source-table dst-dataset]
   (copy-table bq source-dataset source-table dst-dataset nil))
  ([bq source-dataset source-table dst-dataset dst-table]
   (let [source-info (TableId/of source-dataset source-table)
         dst-info (TableId/of dst-dataset (or dst-table source-table))
         copy-cfg (-> (CopyJobConfiguration/newBuilder dst-info source-info)
                      (.build))
         job (.create bq (JobInfo/of copy-cfg) (into-array BigQuery$JobOption []))
         completed-job (.waitFor job (into-array RetryOption []))]
     (when-let [err (some-> completed-job (.getStatus) (.getError))]
       (throw (ex-info "Copy Job Error" {:job completed-job
                                         :status (.getStatus completed-job)
                                         :err err}))))))

#_
(defn clone-table
  [bq source-dataset source-table dst-dataset]
  (let [table-id (TableId/of source-dataset source-table)
        definition (-> (CloneDefinition/newBuilder)
                       (.setBaseTableId table-id)
                       (.build))]
    (create-table bq dst-dataset source-table definition)))