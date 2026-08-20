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

(defn sql-statements
  "Flatten a migration command's SQL into a flat seq of statement strings.

  `generate-sql` yields a vector of strings, but a dialect override -- `:mysql`,
  `:sqlserver` -- is hand-written, and has always been permitted to be either a
  bare string or a vector of strings.  A few of the oldest migrations nest a
  vector inside that vector.  Flattening here guarantees every leaf reaches the
  executor, at whatever depth it was written.

  That nesting used to be worse than mishandled, it was silent.
  `jdbc/db-do-commands` has arglist `[db transaction? & commands]`, so passing a
  vector as the lone command bound `transaction?` and left `commands` empty.
  `executeBatch` then ran against a statement with nothing added to it, reported
  success, and the migration's version row was written regardless -- recording
  the migration as applied when it had never run.

  `context` is an optional `{:version :command}` map, used only to name the
  offending migration when a leaf turns out not to be a string."
  ([sql] (sql-statements sql nil))
  ([sql {:keys [version command] :as context}]
   (cond
     (string? sql)     [sql]
     (sequential? sql) (mapcat #(sql-statements % context) sql)
     (nil? sql)        []
     :else
     (throw (ex-info (str "Migration " (or version "<unknown>")
                          ": SQL must be a string, or a possibly-nested collection "
                          "of strings, but got " (pr-str sql)
                          (when command (str " from command " (pr-str command))))
                     {:version version :command command :sql sql})))))
