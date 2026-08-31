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
    [this from version commands]
    "Apply the given migration and update the schema_versions table accordingly.

    `from` names the version the database is moving away from.  It is nil for a
    forward migration -- the three-argument arity -- which records `version`,
    the migration just applied, in the version table.  For a rollback, `from` is
    the version being rolled back and `version` the version being rolled back
    to, and the row for `from` is deleted instead of a row for `version` being
    inserted: a migration that has been rolled back is no longer applied, so it
    must no longer be recorded as applied."))

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

(defn delete-schema-version
  "SQL to remove a rolled-back migration's row from the version table.

  Rollback ran a migration's `down` commands and then handed the version table
  the version it had rolled back *to*, which was inserted as though it were a
  newly applied migration.  The rolled-back version's own row was never removed,
  so the database went on reporting itself at that version: `pending` skipped
  the migration on the way back up, and a duplicate row accumulated for the
  version below it.  Deleting the row is what makes the rollback visible.

  The version column is a VARCHAR, so the literal is quoted -- PostgreSQL will
  not compare a character column against a bare numeric literal."
  [version-table version]
  (format "DELETE FROM %s WHERE version = '%s'" version-table version))
