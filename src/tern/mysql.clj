(ns tern.mysql
  (:require [tern.db           :refer :all]
            [tern.log          :as log]
            [clojure.java.jdbc :as jdbc]
            [clojure.string    :as s])
  (:import [java.util Date]
           [java.sql PreparedStatement Timestamp]))

(def ^{:doc "Set of supported commands. Used in `generate-sql` dispatch."
       :private true}
  supported-commands
  #{:create-table :drop-table :alter-table :create-index :drop-index})

(defn generate-pk
  [{:keys [primary-key] :as command}]
  (when primary-key
    (format "PRIMARY KEY (%s)" (to-sql-list primary-key))))

(defn generate-table-spec
  [{:keys [constraints] :as command}]
  (let [fks 
        (when constraints
          (for [[constraint & specs] constraints]
            (do
              (format "CONSTRAINT %s FOREIGN KEY %s"
                      (to-sql-name constraint)
                      (s/join " " specs)))))]
    (when fks (s/join " " fks))))

(defmulti generate-sql
  (fn [c] (some supported-commands (keys c))))

(defmethod generate-sql
  :create-table
  [{table :create-table columns :columns :as command}]
  (log/info " - Creating table" (log/highlight (name table)))
  (let [table-spec (generate-table-spec command)
        create-command (if-let [pk (generate-pk command)]
                         (apply jdbc/create-table-ddl table (conj columns [pk]))
                         (apply jdbc/create-table-ddl table columns))]
     [(if table-spec
        (format "%s %s" create-command table-spec)
        create-command)]))

(defmethod generate-sql
  :drop-table
  [{table :drop-table}]
  (log/info " - Dropping table" (log/highlight (name table)))
  [(jdbc/drop-table-ddl table)])

(defmethod generate-sql
  :alter-table
  [{table :alter-table add-columns :add-columns drop-columns :drop-columns modify-columns :modify-columns
          add-constraints :add-constraints  drop-constraints :drop-constraints}]
  (log/info " - Altering table" (log/highlight (name table)))
  (let [additions
        (for [[column & specs] add-columns]
          (do (log/info "    * Adding column" (log/highlight (name column)))
            (format "ALTER TABLE %s ADD COLUMN %s %s"
                    (to-sql-name table)
                    (to-sql-name column)
                    (s/join " " specs))))
        removals
        (for [column drop-columns]
          (do (log/info "    * Dropping column" (log/highlight (name column)))
            (format "ALTER TABLE %s DROP COLUMN %s"
                    (to-sql-name table)
                    (to-sql-name column))))
        modifications
        (for [[column & specs] modify-columns]
          (do (log/info "    * Modifying column" (log/highlight (name column)))
              (format "ALTER TABLE %s MODIFY COLUMN %s %s"
                      (to-sql-name table)
                      (to-sql-name column)
                      (s/join " " specs))))
        new-constraints
        (for [[constraint & specs] add-constraints]
          (do (log/info "    * Adding constraint " (log/highlight (if constraint constraint "unnamed")))
              (format "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY %s" 
                      (to-sql-name table)
                      (to-sql-name constraint)
                      (s/join  " " specs))))
       old-constraints
       (for [constraint drop-constraints]
         (do (log/info "    * Removing constraint " (log/highlight constraint))
             (format "ALTER TABLE %s DROP FOREIGN KEY %s",
                     (to-sql-name table)
                     (to-sql-name constraint))))]
    (doall (concat old-constraints removals additions modifications new-constraints))))

(defmethod generate-sql
  :create-index
  [{index   :create-index
    table   :on
    columns :columns
    unique  :unique}]
  (log/info " - Creating" (if unique "unique" "") "index" (log/highlight (name index)) "on" (log/highlight (name table)))
  [(format "CREATE %s INDEX %s ON %s (%s)"
           (if unique "UNIQUE" "")
           (to-sql-name index)
           (to-sql-name table)
           (s/join ", " (map to-sql-name columns)))])

(defmethod generate-sql
  :drop-index
  [{index :drop-index table :on}]
  (log/info " - Dropping index" (log/highlight (name index)))
  [(format "DROP INDEX %s ON %s" (to-sql-name index) (to-sql-name table))])

(defmethod generate-sql
  :default
  [command]
  (log/error "Don't know how to process command:" (log/highlight (pr-str command)))
  (System/exit 1))

(defn- database-exists?
  [db]
  (jdbc/query
    (db-spec db "mysql")
    ["SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?" (:database db)]
    :result-set-fn first))

(defn- table-exists?
  [db table]
  (jdbc/query
    (db-spec db)
    ["SELECT 1 FROM information_schema.tables WHERE table_name = ?" table]
    :result-set-fn first))

(defn- create-database
  [db]
  (jdbc/db-do-commands
    (db-spec db "mysql") false
    (format "CREATE DATABASE %s" (:database db))))

(defn- create-version-table
  [db version-table]
  (apply jdbc/db-do-commands
         (db-spec db)
         (generate-sql
           {:create-table version-table
            :columns [[:version "VARCHAR(14)" "NOT NULL"]
                      [:created "BIGINT"      "NOT NULL"]]})))

(defn- psql-error-message
  [e]
  (s/replace (.getMessage e) #"^(FATAL|ERROR): " ""))

(defn- batch-update-error-message
  [e]
  (s/replace (.getMessage (.getNextException e)) #"^(FATAL|ERROR): " ""))

(defn- init-db!
  [{:keys [db version-table]}]
  (try
    (when-not (database-exists? db)
      (create-database db)
      (log/info "Created database:" (:database db)))
    (when-not (table-exists? db version-table)
      (create-version-table db version-table)
      (log/info "Created table:   " version-table))))

(defn- get-version
  [{:keys [db version-table]}]
  (try
    (jdbc/query
      (db-spec db)
      [(format "SELECT version FROM %s
                  ORDER BY version DESC
                  LIMIT 1" version-table)]
      :row-fn :version
      :result-set-fn first)))

(defn- update-schema-version
  [version-table version]
  (format "INSERT INTO %s (version) VALUES (%s)"
          version-table version))

(defn- run-migration!
  [{:keys [db version-table]} version commands]
  (when-not (vector? commands)
    (log/error "Values for `up` and `down` must be vectors of commands"))
  (try
    (let [sql-commands (into [] (mapcat generate-sql commands))]
      (log/info "Running: " (s/join "\n" sql-commands))
      (apply jdbc/db-do-commands
							             (db-spec db)
							             (conj sql-commands
							                   (update-schema-version version-table version))))))

(defn- validate-commands
  [commands]
  (cond (and (vector? commands)
             (every? map? commands)) commands
        (map? commands) [vec commands]
        nil nil
        :else (do
                (log/error "The values for `up` and `down` must be either a map or a vector of maps.")
                (System/exit 1))))

(defrecord MysqlMigrator
  [config]
  Migrator
  (init[this]
    (init-db! config))
  (version [this]
    (or (get-version config) "0"))
  (migrate [this version commands]
    (run-migration! config version (validate-commands commands))))
