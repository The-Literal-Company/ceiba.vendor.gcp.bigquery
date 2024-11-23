```clojure
{litco/ceiba.vendor.gcp.bigquery {:git/url "https://github.com/The-Literal-Company/ceiba.vendor.gcp.bigquery.git" 
                                  :git/sha "3ee512bb22274186ef064100798fcaca4daf04f6"}}
```

<hr>

```clojure
(require '[ceiba.vendor.gcp.bigquery :as bq]
         '[integrant.core :as ig])
         
(def dataset
  #:dataset{:gcp/project "your-project"
            :location "us-central1"
            :id "demo"
            :properties {:description "a dataset definition syntax example"
                         :labels {"labels_are" "a_place_to_store_metadata"}}
            :tables [#:table{:id "example_schemaless_table"
                             :description "a table that can recieve json payloads from a pubsub subscription"   
                             :fields [#:field{:type :string
                                              :name "subscription_name"
                                              :mode :nullable}
                                      #:field{:type :string
                                              :name "message_id"
                                              :mode :nullable}
                                      #:field{:type :timestamp
                                              :name "publish_time"
                                              :mode :nullable}
                                      #:field{:type :json
                                              :name "data"
                                              :mode :nullable}
                                      #:field{:type :json
                                              :name "attributes"
                                              :mode :nullable}]}]})
                                              
(def cfg {::bq/client {}
          [:demo/dataset ::bq/dataset] (assoc dataset ::bq/client (ig/ref ::bq/client))}
          
(ig/init cfg) ;=> will build dataset if DNE, else return synchronized representation
```

+ `:ceiba.vendor.gcp.bigquery/client` is an integrant component for constructing a [bigquery client](https://cloud.google.com/java/docs/reference/google-cloud-bigquery/latest/com.google.cloud.bigquery.BigQuery)

+ `:ceiba.vendor.gcp.bigquery/dataset` is an integrant component that synchronizes bigquery datasets from an edn representation
  +  if the dataset does not exist, it will create it. if any table do not exist, it will be created. if any fields are missing from a table schemas, they will be appended
  +  you can sync to an pre-existing dataset (ie created out-of-band) and receive its edn representation in return safely
  +  ceiba uses edn content addressing via [hasch](https://github.com/replikativ/hasch) to efficiently detect drift.
     + at this time, this requires manually tracking mutations in the input edn out-of-band
     + you can force a re-sync with `(assoc dataset ::bq/ignore-cache? true)`
  +  any physical resources that are not present in the schema will be converted into edn form and incorporated into the return schema product. no mutation takes place, and novelty will be represented in the returned specification

+ `:ceiba.vendor.gcp.bigquery/dataset-tables` is an integrant component that synchronizes a subset of the dataset tables
  + this is faster than a full dataset sync, but will not create a dataset if it does not exist
