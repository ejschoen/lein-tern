(ns tern.db
  (:require [tern.misc      :refer :all]
            [clojure.string :as s]))

(defprotocol Migrator
  "Protocol that must be extended by all Migrator instances.
  Provides the base level of functionality required by `tern`."

  (init
    [this]
    "Perform any setup required for tern to work, such as the creation
    of the schema_versions table.")

  (version
    [this]
    "Return the current version of the database.")
  
  (versions
    [this]
    "Return all schema version numbers recorded in the database")
  
  (migrate
    [this version commands]
    "Apply the given migration and update the schema_versions table accordingly."))

(defn subname
  "Build the db subname from its component parts"
  [{:keys [host port database] :as db}]
  (cond (and (not-empty host) (not (nil? port)))
        (str "//" host ":" port "/" database)
        (not-empty host)
        (str "//" host "/" database)
        :else database))

(defn db-spec
  "Build a jdbc compatible db-spec from db config."
  ([db]
   (assoc-result db :subname subname))
  ([db database-override]
   (db-spec (assoc db :database database-override))))

(defn to-sql-name
  "Convert a possibly kebab-case keyword into a snakecase string"
  [k]
  (s/replace (name k) "-" "_"))

(defn to-sql-list
  "Convert a list of possibly kebab cased keys into a list of snakecased strings"
  [ks]
  (s/join ", " (map to-sql-name ks)))
