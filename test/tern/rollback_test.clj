(ns tern.rollback-test
  "The version table has to forget a migration that has been rolled back.

  Rollback used to hand the version table the version it had rolled back *to*,
  which was inserted as though a new migration had just been applied.  Nothing
  ever deleted the rolled-back version's row, so `version` kept reporting the
  database at the version whose `down` commands had just run, `pending` skipped
  that migration on the way back up, and a duplicate row piled up for the
  version below it.  Rollback changed the schema and left no trace of it."
  (:require [tern.db      :refer [Migrator delete-schema-version]]
            [tern.migrate :as migrate]
            [tern.postgresql]
            [tern.mysql]
            [tern.h2v1]
            [tern.h2v2]
            [tern.sqlite]
            [tern.sqlserver]
            [expectations :refer :all])
  (:import [java.nio.file Paths]))

;; --- the statement itself -----------------------------------------------------
;;
;; The version column is a VARCHAR, so the literal is quoted; PostgreSQL refuses
;; to compare a character column against a bare numeric literal.

(expect "DELETE FROM schema_version WHERE version = '20141123233648'"
        (delete-schema-version "schema_version" "20141123233648"))

;; --- every dialect agrees on what a migration records -------------------------

(def ^:private update-schema-version-fns
  {:postgresql #'tern.postgresql/update-schema-version
   :mysql      #'tern.mysql/update-schema-version
   :h2v1       #'tern.h2v1/update-schema-version
   :h2v2       #'tern.h2v2/update-schema-version
   :sqlite     #'tern.sqlite/update-schema-version
   :sqlserver  #'tern.sqlserver/update-schema-version})

(defn- schema-version-sql
  [from version]
  (into {} (map (fn [[engine f]] [engine (f "schema_version" from version)])
                update-schema-version-fns)))

;; A forward migration inserts the version just applied.  The dialects differ
;; only in whether they carry a `created` column.
(expect {:postgresql "INSERT INTO schema_version (version) VALUES (20141123233648)"
         :mysql      "INSERT INTO schema_version (version,created) VALUES (20141123233648,CURRENT_TIMESTAMP)"
         :h2v1       "INSERT INTO schema_version (version,created) VALUES (20141123233648,CURRENT_TIMESTAMP)"
         :h2v2       "INSERT INTO schema_version (version,created) VALUES (20141123233648,CURRENT_TIMESTAMP)"
         :sqlite     "INSERT INTO schema_version (version,created) VALUES (20141123233648,CURRENT_TIMESTAMP)"
         :sqlserver  "INSERT INTO schema_version (version,created) VALUES (20141123233648,CURRENT_TIMESTAMP)"}
        (schema-version-sql nil "20141123233648"))

;; A rollback deletes the rolled-back version's row instead, and inserts nothing
;; for the version it returns to -- that version's own row is already there.
(expect (zipmap (keys update-schema-version-fns)
                (repeat "DELETE FROM schema_version WHERE version = '20141123233648'"))
        (schema-version-sql "20141123233648" "20141117094728"))

;; --- what `migrate` passes to the migrator ------------------------------------

(defrecord RecordingMigrator [config calls]
  Migrator
  (init     [this] nil)
  (version  [this] nil)
  (versions [this] nil)
  (migrate  [this version commands]
    (swap! calls conj {:from nil :version version :commands commands}))
  (migrate  [this from version commands]
    (swap! calls conj {:from from :version version :commands commands})))

(def ^:private config
  {:migration-dir "examples/postgres-project/migrations"})

(defn- migration
  [name]
  (Paths/get (:migration-dir config) (into-array String [name])))

(defn- record
  "Run `f` against a migrator that records its `migrate` calls, and return them."
  [f]
  (let [calls (atom [])]
    (f (->RecordingMigrator config calls))
    @calls))

;; A forward migration has nothing to move away from, so `from` stays nil and
;; the version table gets an insert.
(expect [{:from nil :version "20141102232626"}]
        (map #(dissoc % :commands)
             (record #(migrate/run % (migration "20141102232626-drop-column-from-cat.edn")))))

;; A rollback names the migration being rolled back, which is what makes its row
;; deletable -- `version` alone is the version being returned to.
(expect [{:from "20141102232626" :version "20141102192946"}]
        (map #(dissoc % :commands)
             (record #(migrate/rollback % (migration "20141102232626-drop-column-from-cat.edn")
                                        "20141102192946"))))

;; ...and it still runs the `down` commands it always did.
(expect [{:alter-table :cats
          :add-columns [[:favourite-food "TEXT" "DEFAULT 'fish'"]]}]
        (:commands (first (record #(migrate/rollback % (migration "20141102232626-drop-column-from-cat.edn")
                                                     "20141102192946")))))

;; `reset` walks every migration back down to nothing, so every row goes: each
;; rollback names its own version as `from`, ending at version "0".
(expect [["20141123233648" "20141117094728"]
         ["20141117094728" "20141104091724"]
         ["20141104091724" "20141102232626"]
         ["20141102232626" "20141102192946"]
         ["20141102192946" "20141102101737"]
         ["20141102101737" "0"]]
        (map (juxt :from :version)
             (record #(migrate/reset % "20141123233648"))))
