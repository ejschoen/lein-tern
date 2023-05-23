(ns tern.commands
  (:require [clojure.string       :as str]
            [tern.config          :as config]
            [tern.db              :as db]
            [tern.file            :as f :refer [fname basename]]
            [tern.implementations :as impl]
            [tern.log             :as log]
            [tern.migrate         :as migrate]
            [tern.version         :as tern-version]))

(defn init
  "Creates the table used by `tern` to track versions."
  [config]
  (db/init (impl/factory config)))

(defn version
  "Prints the database's current version."
  [config]
  (log/info "The database is at version" (db/version (impl/factory config))))

(defn versions
  "Prints all schema migration versions recorded in the database."
  [config]
  (doseq [version (db/versions (impl/factory config))]
    (log/info version)))

(defn new-migration
  "Creates a new migration file using the given name.
  It is preceded by a timestamp, so as to preserve ordering."
  [config name]
  (when (empty? name)
    (log/warn "You must specify a name for your migration.")
    (System/exit 1))
  (log/info "Creating:" (f/new-migration config name)))

(defn config
  "Prints the current configuration values used by `tern`."
  [{:keys [version-table migration-dir db color]}]
  (log/info (log/keyword ":migration-dir ") migration-dir)
  (log/info (log/keyword ":version-table ") version-table)
  (log/info (log/keyword ":color         ") color)
  (log/info (log/keyword ":db"))
  (log/info (log/keyword "  :host        ") (:host db))
  (log/info (log/keyword "  :port        ") (:port db))
  (log/info (log/keyword "  :database    ") (:database db))
  (log/info (log/keyword "  :user        ") (:user db))
  (log/info (log/keyword "  :password    ") (:password db))
  (log/info (log/keyword "  :subprotocol ") (:subprotocol db)))

(defn missing
  "Prints the migrations that are missing (i.e., due to branch merging)"
  [config]
  (doseq [version (migrate/missing (impl/factory config) config)]
    (log/info (str version))))

(defn migrate
  "Runs any pending migrations to bring the database up to the latest version.  Optionally pass comma-separated list of versions to apply."
  [config & [only-versions]]

  (log/info "#######################################################")
  (log/info (format "This is tern version %s" tern-version/tern-version))
  (log/info "#######################################################")
  (log/info "")
    
  (let [impl    (impl/factory config)
        from    (db/version impl)
        to      (migrate/version config)
        missing (migrate/missing impl config)
        version-filter (when (and (string? only-versions) (not-empty only-versions))
                         (set (filter identity (map str/trim (str/split only-versions #"[ ,;]")))))
        pending (if version-filter
                  (filter (fn [p] (version-filter (migrate/parse-version (basename p)))) missing)
                  (migrate/pending config from))]

    (when version-filter
      (log/info "#######################################################")
      (log/info "Called with version filter" version-filter)
      (log/info "#######################################################"))

    (log/info "#######################################################")
    (log/info "Migrating from version"
              (log/highlight (if version-filter (migrate/parse-version (basename (first missing))) from))
              "to"
              (log/highlight (if version-filter (migrate/parse-version (basename (last missing))) to)))
    (log/info "#######################################################")
    (doseq [[index ^Path migration] (map-indexed (fn [i m] [i m]) pending)]
      (log/info "Processing" (log/filename (fname migration)) (format "(%d of %d)" (inc index) (count pending)))
      (if (System/getenv "TERN_DRYRUN")
        (log/info "DRY-RUN is on")
        (migrate/run impl migration)))
    (if (not-empty pending)
      (log/info "Migration complete")
      (log/info "There were no changes to apply"))))

(defn rollback
  "Rolls back the most recent migration"
  [config]
  (let [impl      (impl/factory config)
        from      (db/version impl)
        to        (migrate/previous-version config from)]
    (log/info "#######################################################")
    (log/info "Rolling back from version" (log/highlight from) "to" (log/highlight to))
    (log/info "#######################################################")
    (if-let [migration (migrate/get-migration config from)]
      (do
        (migrate/rollback impl migration to)
        (log/info "Rollback complete"))
      (log/info "There were no changes to roll back"))))

(defn reset
  "Reverts all migrations, returning database to it's original state."
  [config]
  (println "Are you sure? This will roll back ALL migrations. y/n")
  (when (= "y" (read-line))
    (let [impl    (impl/factory config)
          version (db/version impl)]
      (log/info "#######################################################")
      (log/info (log/danger "Rolling back ALL migrations"))
      (log/info "#######################################################")
      (migrate/reset impl version)
      (log/info "Reset complete"))))
