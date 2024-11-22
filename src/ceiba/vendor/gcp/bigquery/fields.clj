(ns ceiba.vendor.gcp.bigquery.fields
  (:import (com.google.cloud.bigquery Field
                                      Field$Mode
                                      StandardSQLTypeName
                                      LegacySQLTypeName)))

(def LegacySQL->keyword
  {LegacySQLTypeName/STRING :string
   LegacySQLTypeName/BYTES :bytes
   LegacySQLTypeName/INTEGER :int64
   LegacySQLTypeName/FLOAT :float64
   LegacySQLTypeName/NUMERIC :numeric
   LegacySQLTypeName/BIGNUMERIC :bignumeric
   LegacySQLTypeName/BOOLEAN :bool
   LegacySQLTypeName/JSON :json
   LegacySQLTypeName/RECORD :record
   LegacySQLTypeName/TIMESTAMP :timestamp
   LegacySQLTypeName/DATE :date
   LegacySQLTypeName/TIME :time
   LegacySQLTypeName/DATETIME :datetime})

(def SQL->keyword
  {StandardSQLTypeName/ARRAY :array
   StandardSQLTypeName/BIGNUMERIC :bignumeric
   StandardSQLTypeName/BOOL :bool
   StandardSQLTypeName/BYTES :bytes
   StandardSQLTypeName/DATE :date
   StandardSQLTypeName/DATETIME :datetime
   StandardSQLTypeName/FLOAT64 :float64
   StandardSQLTypeName/GEOGRAPHY :geography
   StandardSQLTypeName/INT64 :int64
   StandardSQLTypeName/INTERVAL :interval
   StandardSQLTypeName/JSON :json
   StandardSQLTypeName/NUMERIC :numeric
   StandardSQLTypeName/RANGE :range
   StandardSQLTypeName/STRING :string
   StandardSQLTypeName/STRUCT :struct
   StandardSQLTypeName/TIME :time
   StandardSQLTypeName/TIMESTAMP :timestamp})

(def keyword->SQL
  {:array StandardSQLTypeName/ARRAY
   :bignumeric StandardSQLTypeName/BIGNUMERIC
   :bool StandardSQLTypeName/BOOL
   :bytes StandardSQLTypeName/BYTES
   :date StandardSQLTypeName/DATE
   :datetime StandardSQLTypeName/DATETIME
   :float64 StandardSQLTypeName/FLOAT64
   :geography StandardSQLTypeName/GEOGRAPHY
   :int64 StandardSQLTypeName/INT64
   :interval StandardSQLTypeName/INTERVAL
   :json StandardSQLTypeName/JSON
   :numeric StandardSQLTypeName/NUMERIC
   :range StandardSQLTypeName/RANGE
   :string StandardSQLTypeName/STRING
   :struct StandardSQLTypeName/STRUCT
   :time StandardSQLTypeName/TIME
   :timestamp StandardSQLTypeName/TIMESTAMP})

(def Mode->keyword
  {Field$Mode/NULLABLE :nullable
   Field$Mode/REQUIRED :required
   Field$Mode/REPEATED :repeated})

(def keyword->Mode
  {:nullable Field$Mode/NULLABLE
   :required Field$Mode/REQUIRED
   :repeated Field$Mode/REPEATED})

#!=============================================================================
#! field-spec
{:field/name :string
 :field/type :keyword
 :field/mode #{:nullable :repeated :required}
 :field/description :string
 :field/fields [:maybe [:sequential :field/spec]]}
#!=============================================================================

(defn field-to-spec [f]
  (cond->   ;; TODO nested fields
    {:field/name (.getName f)
     :field/type (or (LegacySQL->keyword (.getType f)) (.getType f))
     :field/mode (Mode->keyword (.getMode f))}
    (.getDescription f) (assoc :field/description (.getDescription f))
    (.getDefaultValueExpression f) (assoc :field/default (.getDefaultValueExpression f))))

(defn ^Field field-from-spec
  [{mode :field/mode
    type :field/type
    name :field/name
    description :field/description
    default :field/default
    fields :field/fields
    :or {mode :nullable}}]
  (assert (and (some? type) (keyword? type)))
  (assert (contains? keyword->SQL type)
          (str "field " name  " has invalid field :type key " type ", must be one-of " (keys keyword->SQL)))
  (assert (and (some? mode) (keyword? mode) (contains? keyword->Mode mode)))
  (let [sub-fields (into-array Field (map field-from-spec fields))
        mode (or (keyword->Mode mode) (throw (Exception. ^String (str "unknown :field/mode key" mode))))
        builder (Field/newBuilder ^String name
                                  ^StandardSQLTypeName (keyword->SQL type)
                                  ^Field/1 sub-fields)]
    (when description
      (.setDescription builder description))
    (when default
      (.setDefaultValueExpression builder default))
    (.setMode builder mode)
    (.build builder)))
