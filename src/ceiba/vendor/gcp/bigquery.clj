(ns ceiba.vendor.gcp.bigquery
  (:require [ceiba.vendor.gcp.bigquery.client :as client]
            [ceiba.vendor.gcp.bigquery.dataset :as dataset]
            [integrant.core :as ig])
  (:import [com.google.cloud.bigquery BigQuery]))

(defmethod ig/init-key ::client [_ opts] ^BigQuery (client/build opts))

(defmethod ig/init-key ::dataset [_ dataset-ctx] (dataset/sync dataset-ctx))

(defmethod ig/init-key ::dataset-tables [_ dataset-ctx] (dataset/sync-tables dataset-ctx))

(def query client/query)

(def copy-table client/copy-table)

(def ^{:doc "insert rows into the streaming-buffer"}
  insert-rows client/insert-rows)

(def delete-table client/delete-table)

(def delete-dataset client/delete-dataset)

(def list-datasets client/list-datasets)

#!-----------------------------------------------------------------------------
#! TODO
#! ::job
#! Clone Table
#! storage client
#! Routines
#! Tables
#!   - Materialized View
#!   - Snapshot(Table)
#!   - Models
#!   - external
;; TODO unimplemented properties; dataset.toBuilder()
;setAcl(List<Acl> acl)
;setDefaultCollation(String defaultCollation)
;setDefaultEncryptionConfiguration(EncryptionConfiguration defaultEncryptionConfiguration)
;setDefaultPartitionExpirationMs(Long defaultPartitionExpirationMs)
;setDefaultTableLifetime(Long defaultTableLifetime)
;setExternalDatasetReference(ExternalDatasetReference externalDatasetReference)
;setFriendlyName(String friendlyName)
;setStorageBillingModel(String storageBillingModel)
