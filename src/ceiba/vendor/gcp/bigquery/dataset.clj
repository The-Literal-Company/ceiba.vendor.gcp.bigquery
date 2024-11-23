(ns ceiba.vendor.gcp.bigquery.dataset
  (:refer-clojure :exclude [sync])
  (:require [ceiba.vendor.gcp.bigquery.client :as client]
            [ceiba.vendor.gcp.bigquery.fields :as fields]
            [clojure.data :refer [diff]]
            [clojure.string :as string]
            [hasch.core :as hasch]
            [taoensso.telemere :as t])
  (:import (com.google.cloud.bigquery BigQuery$DatasetOption BigQuery$TableOption ColumnReference DatasetInfo Field ForeignKey PrimaryKey Schema StandardTableDefinition TableConstraints TableDefinition$Type TableId TableInfo ViewDefinition)
           (java.util ArrayList)))

(defn- column-reference
  [{:keys [referenced-col-name referencing-col-name]}]
  (-> (ColumnReference/newBuilder)
      (.setReferencedColumn referenced-col-name)
      (.setReferencingColumn referencing-col-name)
      (.build)))

(defn- foreign-key
  [project-id dataset-id {:keys [referenced-table column-references]}]
  (let [column-references (map column-reference column-references)]
    (-> (ForeignKey/newBuilder)
        (.setReferencedTable (TableId/of project-id dataset-id referenced-table))
        (.setColumnReferences (doto (ArrayList.) (.addAll column-references)))
        (.build))))

(defn- build-primary-keys [columns]
  (let [lst (ArrayList.)]
    (doseq [col columns]
      (.add lst col))
    (-> (PrimaryKey/newBuilder)
        (.setColumns lst)
        (.build))))

(defn- build-table-constraints
  [project-id dataset-id {:keys [primary-keys foreign-keys]}]
  (let [constraints-builder (TableConstraints/newBuilder)]
    (when (seq primary-keys)
      (.setPrimaryKey constraints-builder (build-primary-keys primary-keys)))
    (when (seq foreign-keys)
      (let [fks (map (partial foreign-key project-id dataset-id) foreign-keys)]
        (.setForeignKeys constraints-builder (doto (ArrayList.) (.addAll fks)))))
    (.build constraints-builder)))

(defn- spec->table
  [{bq         :ceiba.vendor.gcp.bigquery/client
    project-id :gcp/project
    dataset-id :dataset/id}
   {table-id    :table/id
    description :table/description
    table-type  :table/type
    fields      :table/fields
    constraints :table/constraints
    view-sql    :view/sql
    :as         table-spec}]
  (assert (some? project-id))
  (assert (some? dataset-id))
  (assert (contains? table-spec :table/type) (str "table-spec for " (pr-str table-id) " is missing :table/type"))
  (let [definition (cond
                     (= :standard table-type)
                     (-> (StandardTableDefinition/newBuilder)
                         (.setSchema (Schema/of ^Field/1 (into-array Field (map fields/field-from-spec fields))))
                         (.setTableConstraints (build-table-constraints project-id dataset-id constraints))
                         (.build))

                     (= :view table-type)
                     (.build (ViewDefinition/newBuilder view-sql))

                     :else
                     (throw (Exception. ^String (str "unimplemented table type:" table-type))))
        table-id (TableId/of dataset-id table-id)
        table-opts (into-array BigQuery$TableOption [])
        table-info-builder (TableInfo/newBuilder table-id definition)]
    (when description
      (.setDescription table-info-builder description))
    (.create bq ^TableInfo (.build table-info-builder) ^BigQuery$TableOption/1 table-opts)))

(defn- constraints->spec
  [constraints]
  (let [fks (.getForeignKeys constraints)]
    (when (seq fks)
      (throw (ex-info "foreign key recovery unimplemented!" {:constraints constraints})))
    {:primary-keys (vec (.getColumns (.getPrimaryKey constraints)))}))

(defn- spec-from-table-id
  [{bq         :ceiba.vendor.gcp.bigquery/client
    dataset-id :dataset/id}
   table-id]
  (let [t (client/get-table bq dataset-id table-id)
        constraints (.getTableConstraints t)
        definition (.getDefinition t)
        fields (mapv fields/field-to-spec (.getFields (.getSchema definition)))
        table-type (condp = (.getType definition)
                     TableDefinition$Type/VIEW :view
                     TableDefinition$Type/MATERIALIZED_VIEW :materialized-view
                     TableDefinition$Type/EXTERNAL :external
                     TableDefinition$Type/SNAPSHOT :snapshot
                     TableDefinition$Type/MODEL :model
                     TableDefinition$Type/TABLE :standard
                     (throw (ex-info (str "unknown table type: " (.getType definition)) {:definition definition
                                                                                         :table-id table-id
                                                                                         :dataset-id dataset-id})))]
    (cond-> {:table/id          table-id
             :table/type        table-type
             :table/description (.getDescription t)
             :table/fields      fields}
            (some? constraints) (assoc :table/constraints (constraints->spec constraints)))))

(defn- clean-novel-field-spec
  "appended columns cannot have defaults or be required"
  [{mode :field/mode :as spec}]
  (let [m (dissoc spec :field/default)]
    (if (= :required mode)
      (assoc m :field/mode :nullable)
      m)))

(defn- sync-table
  "Uses reflection to reconcile the in-memory schema with the physical schema.

   Is purely accretive, append only:
   - 'append': A field is not physically present, that column is appended to the physical table
   - 'untracked' : A physical column is not in the in-mem spec, it is included in return fields

   Novel fields will be stripped of 'required' mode & 'default' value expression. Reconciled fields
   in these situations will always come back modified.

   Returns possibly updated table spec"
  [{bq :ceiba.vendor.gcp.bigquery/client
    dataset-id :dataset/id}
   {table-id :table/id :as table-spec}]
  {:pre [(some? dataset-id) (some? table-id)]
   :post [(map? %) (contains? % :table/id) (contains? % :table/type)]}
  (t/log! (str "---- starting table schema sync for " dataset-id "." table-id))
  (let [table (client/get-table bq dataset-id table-id)
        _ (assert (some? table))
        definition (.getDefinition table)
        actual-fields (let [schema (.getSchema definition)]
                        (assert (some? schema))
                        (mapv fields/field-to-spec (.getFields schema)))
        ?actual-view-query
        (when (= TableDefinition$Type/VIEW (.getType definition))
          (let [expected-query (:view/sql table-spec)
                actual-query (.getQuery ^ViewDefinition definition)]
            (when (not= expected-query actual-query)
              (t/log! :warn (str "------ found View query drift " dataset-id "." table-id))
              (t/log! :warn (str "-------- expected " (pr-str expected-query)))
              (t/log! :warn (str "--------   actual " (pr-str actual-query)))
              actual-query)))
        table-spec (cond-> table-spec
                           ?actual-view-query (assoc :view/sql ?actual-view-query))]
    ;;
    ;; TODO enforce view-fields do no support :field/description, :field/mode cannot be :required
    (if (= (set (:table/fields table-spec)) (set actual-fields))
      (do
        (t/log! (str "---- finished syncing table schema for " dataset-id "." table-id))
        table-spec)
      (let [_(t/log! :warn (str "------ found schema drift for " dataset-id "." table-id))
            [append-columns untracked-columns ] (diff (set (:table/fields table-spec)) (set actual-fields))]
        ;;
        ;; TODO 'required' tracked columns that are present but nullable will cause blowup past this pt
        ;;  ... this happens easily when appending 'required' column to existing table, that mode is stripped
        ;;  FIX: clean append & untracked, look for intersection, filter from update call below and warn user,
        ;;  return nullable table back
        ;;
        ;; TODO similarly, entirely omitted :mode will trigger blowup when in comes back as :nullable but
        ;; otherwise isnt present in provided spec
        ;;
        ;; maybe better to blow up on appending required instead of tolerating?
        ;;
        (when untracked-columns
          (t/log! :warn (str "-------- found untracked columns: " (pr-str (mapv :field/name untracked-columns)))))
        (if (nil? append-columns)
          (do
            (t/log! (str "---- finished syncing table schema for " dataset-id "." table-id))
            (assoc table-spec :table/fields actual-fields))
          (let [_(t/log! :warn (str "------ found columns missing, appending " (pr-str (mapv :field/name append-columns))))
                next-fields (into actual-fields (map clean-novel-field-spec) append-columns)
                schema (Schema/of ^Field/1 (into-array ^Field (map fields/field-from-spec next-fields)))
                definition (StandardTableDefinition/of schema)]
            (-> (.toBuilder table)
                (.setDefinition definition)
                (.build)
                (.update (into-array BigQuery$TableOption [])))
            (t/log! (str "---- finished syncing table schema for " dataset-id "." table-id))
            (assoc table-spec :table/fields next-fields)))))))

(defn- remove-ceiba-labels [labels]
  (when (seq labels)
    (into {}
          (map
            (fn [[k :as entry]]
              (when-not (string/starts-with? k "ceiba")
                entry)))
          (seq labels))))

(defn clean-dataset-labels [ctx]
  (if-some [labels (get-in ctx [:dataset/properties :labels])]
    (if-some [cleaned (remove-ceiba-labels labels)]
      (assoc-in ctx [:dataset/properties :labels] cleaned)
      (update ctx :dataset/properties dissoc :labels))
    (update ctx :dataset/properties dissoc :labels)))

(defn hash-table [table]
  (str (hasch/uuid table)))

(defn hash-many-tables [tables]
  (when tables
    (assert (vector? tables))
    (assert (every? map? tables))
    (str (hasch/uuid tables))))

(defn hash-props [props]
  (str (hasch/uuid (update props :labels remove-ceiba-labels))))

(defn select-dataset [ctx]
  (-> ctx
      clean-dataset-labels
      (select-keys [:dataset/name
                    :dataset/location
                    :dataset/description
                    :dataset/tables])))

(defn hash-dataset [ctx]
  (str (hasch/uuid (select-dataset ctx))))

(defn dataset-info->props [dataset-info]
  {:acl (.getAcl dataset-info)
   :dataset-id (.getDatasetId dataset-info)
   :default-collation (.getDefaultCollation dataset-info)
   :default-encryption-configuration (.getDefaultEncryptionConfiguration dataset-info)
   :default-partition-expiration-ms (.getDefaultPartitionExpirationMs dataset-info)
   :default-table-lifetime (.getDefaultTableLifetime dataset-info)
   :description (.getDescription dataset-info)
   :external-dataset-reference (.getExternalDatasetReference dataset-info)
   :friendly-name (.getFriendlyName dataset-info)
   :labels (.getLabels dataset-info)
   :location (.getLocation dataset-info)
   :storage-billing-model (.getStorageBillingModel dataset-info)
   :etag (.getEtag dataset-info)
   :creation-time (.getCreationTime dataset-info)
   :generated-id (.getGeneratedId dataset-info)
   :last-modified (.getLastModified dataset-info)
   :self-link (.getSelfLink dataset-info)})

(defn- table-label
  [{table-id :table/id}]
  (str "ceiba_table_hash_" (string/lower-case table-id)))

(defn- sync-dataset-tables
  "(ctx) => full rescan
   (ctx, labels) => defaults all tables, skips those w/ matching hasch
   (ctx, labels, tables) => only checks provided tables, skips those w/ matching hasch"
  ([ctx]
   (sync-dataset-tables ctx {}))
  ([ctx labels]
   (sync-dataset-tables ctx labels any?))
  ([{bq          :ceiba.vendor.gcp.bigquery/client
     dataset-id  :dataset/id
     table-specs :dataset/tables :as ctx}
    labels
    table-id-filter]
   {:pre [(some? bq)]
    :post [(every? map? %)]}
   (t/log! (str "-- synchronizing tables for " dataset-id))
   (let [actual-table-id-set (into #{} (comp (map #(.getTable (.getTableId %)))
                                             (filter table-id-filter)) (client/list-tables bq dataset-id))
         [novel-names untracked-names common-names] (diff (set (map :table/id table-specs)) actual-table-id-set)]
     (let [novel-specs
           (mapv
             (fn [{table-id :table/id :as table-spec}]
               (t/log! (str "---- creating " dataset-id "." table-id))
               (spec->table ctx table-spec)
               table-spec)
             (filter #(contains? novel-names (:table/id %)) table-specs))
           _(assert (every? map? novel-specs))
           common-specs
           (mapv
             (fn [table-spec]
               (if (= (hash-table table-spec) (get labels (table-label table-spec)))
                 (do
                   (t/log! (str "---- hashes match for " dataset-id "." (:table/id table-spec)))
                   table-spec)
                 (sync-table ctx table-spec)))
             (filter #(contains? common-names (:table/id %)) table-specs))
           untracked-specs
           (mapv
             (fn [table-id]
               (t/log! :warn (str "---- found untracked table, incorporating table '" table-id "'"))
               (spec-from-table-id ctx table-id))
             untracked-names)]
       (t/log! (str "-- finished synchronizing tables for " dataset-id))
       (vec (sort-by :table/id (concat novel-specs common-specs untracked-specs)))))))

(defn- next-props
  "no io calls, no writes, remote precedence"
  [dataset-info {:keys [labels description]}]
  (let [remote-description (.getDescription dataset-info)
        remote-labels (remove-ceiba-labels (.getLabels dataset-info))
        labels (merge labels remote-labels)
        description (or remote-description description)]
    (when (or (seq labels) (some? description))
      (cond-> {}
              labels (assoc :labels labels)
              description (assoc :description description)))))

(defn sync
  "syncs complete dataset via dataset/tables and dataset/properties hasching"
  [{bq            :ceiba.vendor.gcp.bigquery/client
    ignore-cache? ::client/ignore-cache?
    project-id    :gcp/project
    location      :dataset/location
    dataset-id    :dataset/id
    :as           ctx}]
  {:pre [(contains? ctx :ceiba.vendor.gcp.bigquery/client)
         (contains? ctx :gcp/project)
         (contains? ctx :dataset/id)
         (contains? ctx :dataset/location)]}
  (if-let [dataset (client/get-dataset bq dataset-id)] ;;<-- the minimal IO op
    (let [labels (cond-> (.getLabels dataset) ignore-cache? remove-ceiba-labels)
          tables-hash-expected (hash-many-tables (:dataset/tables ctx))]
      (t/log! (str "using existing dataset: " project-id "." dataset-id " in " (.getLocation ^DatasetInfo dataset)))
      (if (= (hash-dataset ctx) (get labels "ceiba_dataset_hash"))
        (do
          (t/log! (str "hashes match for " dataset-id))
          ctx)
        ;; compare :dataset/properties & :dataset/tables independently
        ;; but we only want to write properties (w/ labels!) ONCE
        (if (not= tables-hash-expected (get labels "ceiba_dataset_tables_hash"))
          ;;
          ;; tables out of sync
          (let [next-tables (sync-dataset-tables ctx labels) ;;<-- expensive
                next-tables-hash (hash-many-tables next-tables)
                {next-user-labels :labels, :as next-props} (next-props dataset (get ctx :dataset/properties))
                next-props-hash (hash-props next-props)
                next-ctx (cond-> (dissoc ctx :dataset/properties)
                                 (seq next-tables) (assoc :dataset/tables next-tables)
                                 (seq next-props) (assoc :dataset/properties next-props))
                next-dataset-hash (hash-dataset next-ctx)
                builder (.toBuilder dataset)
                next-labels (-> next-user-labels
                                (assoc "ceiba_dataset_tables_hash" next-tables-hash
                                       "ceiba_props_hash" next-props-hash
                                       "ceiba_dataset_hash" next-dataset-hash)
                                (into (map (juxt table-label hash-table)) next-tables))]
            (.setLabels builder next-labels)
            (when-let [description (get next-props :description)]
              (.setDescription builder description))
            (.update bq (.build builder) ^BigQuery$DatasetOption/1 (into-array BigQuery$DatasetOption []))
            (t/log! (str "finished synchronizing " dataset-id))
            next-ctx)
          ;;
          ;; tables are same but properties need update
          (let [_ (t/log! (str "tables ok, reconciling properties for " dataset-id))
                builder (.toBuilder dataset)
                {next-user-labels :labels, :as next-props} (next-props dataset (get ctx :dataset/properties))
                next-props-hash (hash-props next-props)
                next-ctx (cond-> (dissoc ctx :dataset/properties)
                                 (seq next-props) (assoc :dataset/properties next-props))
                next-dataset-hash (hash-dataset next-ctx)
                next-labels (-> next-user-labels
                                (assoc "ceiba_dataset_tables_hash" tables-hash-expected
                                       "ceiba_props_hash" next-props-hash
                                       "ceiba_dataset_hash" next-dataset-hash)
                                (into (map (juxt table-label hash-table))
                                      (:dataset/tables ctx)))]
            (.setLabels builder next-labels)
            (when-let [description (get next-props :description)]
              (.setDescription builder description))
            (.update bq (.build builder) ^BigQuery$DatasetOption/1 (into-array BigQuery$DatasetOption []))
            (t/log! (str "finished synchronizing " dataset-id))
            next-ctx))))
    ;;
    ;; clean slate
    (let [_ (t/log! (str "creating dataset with id " dataset-id))
          builder (DatasetInfo/newBuilder (:gcp/project ctx) dataset-id)
          tables-hash (hash-many-tables (:dataset/tables ctx))
          props-hash (hash-props (:dataset/properties ctx))
          dataset-hash (hash-dataset ctx)
          next-labels (-> (or (get-in ctx [:dataset/properties :labels]) {})
                          (assoc "ceiba_dataset_tables_hash" tables-hash
                                 "ceiba_props_hash" props-hash
                                 "ceiba_dataset_hash" dataset-hash)
                          (into (map (juxt table-label hash-table))
                                (:dataset/tables ctx)))]
      (.setLocation builder location)
      (.setLabels builder next-labels)
      (when-let [description (get next-props :description)]
        (.setDescription builder description))
      (.create bq (.build builder) ^BigQuery$DatasetOption/1 (into-array BigQuery$DatasetOption []))
      (sync-dataset-tables ctx nil)
      (t/log! (str "finished synchronizing " dataset-id))
      ctx)))

(defn sync-tables
  "ignores properties, only syncs individual provided tables"
  [{bq            :ceiba.vendor.gcp.bigquery/client
    ignore-cache? :ceiba.vendor.gcp.bigquery/ignore-cache?
    project-id    :gcp/project
    location      :dataset/location
    dataset-id    :dataset/id :as ctx}]
  {:pre [(contains? ctx :ceiba.vendor.gcp.bigquery/client)
         (contains? ctx :gcp/project)
         (contains? ctx :dataset/location)
         (contains? ctx :dataset/id)]}
  (if-let [dataset (client/get-dataset bq dataset-id)] ;;<-- the minimal IO op
    (let [_(t/log! (str "syncing existing dataset-subset " project-id "." dataset-id " in " (.getLocation ^DatasetInfo dataset)))
          labels (cond-> (.getLabels dataset) ignore-cache? remove-ceiba-labels)
          expected-table-hashes (into {} (map (juxt table-label hash-table)) (:dataset/tables ctx))
          actual-hashes (select-keys labels (keys expected-table-hashes))]
      (if (= expected-table-hashes actual-hashes)
        (do
          (t/log! (str "finished synchronizing subset of" dataset-id))
          ctx)
        (let [next-tables (sync-dataset-tables ctx labels (into #{} (map :table/id (:dataset/tables ctx)))) ;;<-- expensive
              {next-user-labels :labels, :as next-props} (next-props dataset (get ctx :dataset/properties))
              next-props-hash (hash-props next-props)
              next-ctx (cond-> (dissoc ctx :dataset/properties)
                               (seq next-tables) (assoc :dataset/tables next-tables)
                               (seq next-props) (assoc :dataset/properties next-props))
              builder (.toBuilder dataset)
              next-labels (-> next-user-labels
                              (assoc "ceiba_dataset_tables_hash" nil ;; we know nothing of other tables
                                     "ceiba_dataset_hash" nil
                                     "ceiba_props_hash" next-props-hash)
                              (into (map (juxt table-label hash-table)) next-tables))]
          (.setLabels builder next-labels)
          (when-let [description (get next-props :description)]
            (.setDescription builder description))
          (.update bq (.build builder) ^BigQuery$DatasetOption/1 (into-array BigQuery$DatasetOption []))
          (t/log! (str "finished synchronizing " dataset-id))
          next-ctx)))
    (throw (Exception. ^String (str "dataset " dataset-id "does not exist")))))

#_
(defn conform-dataset
  "Conforms one dataset shape into another.
   find table schema diffs, deletes untracked, appends missing.
   deletes untracked tables"
  [synced proof])