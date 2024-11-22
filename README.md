```clojure
{litco/ceiba.vendor.gcp.bigquery {:git/url "https://github.com/The-Literal-Company/ceiba.vendor.gcp.bigquery.git" 
                                  :git/sha "954428a15049fe0cfbd79cc4a9353c7ca143af6c"}}
```


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