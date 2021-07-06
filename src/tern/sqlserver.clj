(ns tern.sqlserver
  (:use [tern.db :exclude [to-sql-name]])
  (:require [tern.log          :as log]
            [clojure.java.jdbc :as jdbc]
            [clojure.string    :as s])
  (:import [java.util Date]
           [java.sql PreparedStatement Timestamp]))

(declare primary-key-exists? get-primary-key foreign-key-exists? index-exists? column-exists? table-exists?)

(def ^:dynamic *db* nil)
(def ^:dynamic *plan* nil)

(def ^{:doc "Set of supported commands. Used in `generate-sql` dispatch."
       :private true}
  supported-commands
  #{:create-table :drop-table :alter-table :create-index :drop-index :insert-into :update})

(def reserved-words
  #{"public" "user"})

(defn to-sql-name
  "Convert a possibly kebab-case keyword into a snakecase string"
  [k & {:keys [quote-for-statement] :or {quote-for-statement true}}]
  (let [n (s/replace (name k) "-" "_")]
    (if (and quote-for-statement (reserved-words n))
      (str "[" n "]")
      n)))

(defn generate-pk
  [{:keys [primary-key] :as command}]
  (when primary-key
    (format "PRIMARY KEY (%s)" (to-sql-list primary-key))))

(defn generate-constraints
  [{:keys [constraints] :as command}]
  (let [fks 
        (when constraints
          (for [[constraint & specs] constraints]
            (do
              (format "CONSTRAINT %s FOREIGN KEY %s"
                      (to-sql-name constraint)
                      (s/join " " specs)))))]
    (when fks (map (fn [fk] [fk]) fks))))


(def ignore-options #{"row_format"})


(defn generate-options
  [table-options]
  (when (not-empty table-options)
    (s/join ", "
            (for [option table-options
                  :when (not (get ignore-options (:name option)))]
              (format "%s=%s" (:name option) (:value option))))))

(defmulti generate-sql
  (fn [c] (some supported-commands (keys c))))

(def column-spec-map
  {"auto_increment" "identity"
   "blob" "varbinary(max)"
   "boolean" "bit"
   "character set utf8" ""
   "character set 'utf8'" ""
   "collate utf8_general_ci" ""
   "collate utf8_bin" ""
   "default true" "default 'true'"
   "double" "float"
   "longblob" "varbinary(max)"
   "longtext" "varchar(max)"
   "mediumtext" "varchar(max)"
   "shorttext" "varchar(max)"
   "text" "varchar(max)"
   "timestamp" "datetime"
   })

(def column-spec-pattern-mappings
  [[#"(?i)int\(\d+\)" "int"]
   [#"(?i)tinyint\(1\)" "bit"]
   [#"(?i)tinyint\(\d+\)" "tinyint"]
   [#"(?i)double" "float"]
   [#"^\(\d+\)$" ""]
   (fn [spec column-name]
     (if-let [[_ columns] (re-matches #"(?)ENUM\(([^\)]+)\)" spec)]
       (let [column-strings (map #(subs % 1 (dec (count %))) (s/split columns #","))
             field-size (apply max (map count column-strings))]
         (format "VARCHAR(%d) CHECK (%s IN(%s))" field-size (name column-name) columns))
       spec))
   (fn [spec column-name]
     (if-let [[_ size-str] (re-matches #"(?i)VARBINARY\((\d+)\)" spec)]
       (let [size (Integer/parseInt size-str)]
         (if (> size 8000)
           "varbinary(max)"
           spec))
       spec))
   ])

(defn map-column-spec-token
  [token column-name]
  (-> token
      ((fn [d] (column-spec-map (s/lower-case d) d)))
      ((fn [d] (reduce (fn [d replacement]
                         (if (fn? replacement)
                           (replacement d column-name)
                           (let [[pattern replacement] replacement]
                             (s/replace d pattern replacement))))
                       d
                       column-spec-pattern-mappings)))))

(defn map-column-specs
  [column-specs]
  (for [[name & rest] column-specs]
    (into [] (cons name (map #(map-column-spec-token % name) rest)))))

(defmethod generate-sql
  :create-table
  [{table :create-table columns :columns table-options :table-options :as command}]
  (log/info " - Creating table" (log/highlight (name table)))
  (let [constraints (generate-constraints command)
        table-spec (generate-options table-options)
        pk (generate-pk command)]
    (if (and *db*
             (table-exists? *db* (to-sql-name table :quote-for-statement false))
             (not (some (fn [prior] (= prior {:drop-table table})) @*plan*)))
      (do (log/info (format "  * Skipping CREATE TABLE %s because it already exists." (to-sql-name table)))
          nil)
      (if (not-empty table-spec)
        (concat
         (apply conj
                (generate-sql {:create-table table :columns [[:__placeholder "int"]]})
                (generate-sql {:alter-table table
                               :table-options table-options
                               :add-columns columns
                               :add-constraints (:constraints command)}))
         (apply conj
                [(format "ALTER TABLE %s ADD %s" (to-sql-name table) pk)]
                (generate-sql {:alter-table table
                               :drop-columns [:__placeholder]})))
        [(if pk
           (apply jdbc/create-table-ddl table (apply conj (map-column-specs columns) [pk] constraints))
           (if constraints
             (apply jdbc/create-table-ddl table (apply conj (map-column-specs columns) constraints))
             (apply jdbc/create-table-ddl table (map-column-specs columns))))]))))

(defmethod generate-sql
  :drop-table
  [{table :drop-table}]
  (log/info " - Dropping table" (log/highlight (name table)))
  [(jdbc/drop-table-ddl table)])

(defmethod generate-sql
  :alter-table
  [{table :alter-table add-columns :add-columns drop-columns :drop-columns modify-columns :modify-columns modify-column :modify-column
    add-constraints :add-constraints  drop-constraints :drop-constraints drop-indices :drop-index
    primary-key :primary-key
    table-options :table-options}]
  (log/info " - Altering table" (log/highlight (name table)))
  (let [additions
        (filter identity
                (for [[column & specs] (map-column-specs add-columns)]
                  (if (and *db*
                           (column-exists? *db* (to-sql-name table :quote-for-statement false) (to-sql-name column :quote-for-statement false))
                           (not (some (fn [prior]
                                        (and (= table (:alter-table prior))
                                             (some (fn [col] (= col column)) (:drop-columns prior))))
                                      @*plan*)))
                    (do (log/info (format "   * Skipping ADD COLUMN %s.%s because it already exists."
                                          (to-sql-name table) (to-sql-name column)))
                        nil)
                    (do (log/info "    * Adding column" (log/highlight (name column)))
                        (format "%s %s"
                                (to-sql-name column)
                                (s/join " " specs))))))
        removals
        (filter identity
                (for [column drop-columns]
                  ;; Note: No test for prior alter table/add column or create table w/ column
                  ;; because frankly it would not make sense in a single migration to create
                  ;; a table column and then immediately drop it.
                  (if (or (not *db*)
                          (column-exists? *db* (to-sql-name table :quote-for-statement false) (to-sql-name column :quote-for-statement false)))
                    (do (log/info "    * Dropping column" (log/highlight (name column)))
                        (to-sql-name column))
                    (do (log/info (format "   * Skipping DROP COLUMN %s.%s because that column does not exist."
                                          (to-sql-name table) (to-sql-name column)))
                        nil))))
        modification
        (if modify-column
          (let [[column & specs] modify-column]
            (do (log/info "    * Modifying column" (log/highlight (name column)))
                [(format "ALTER COLUMN %s %s"
                         (to-sql-name column)
                         (s/join " " (map #(map-column-spec-token % column) specs)))]))
          nil)
        primary-key-constraints
        (if primary-key
          [(generate-pk {:primary-key primary-key})]
          [])
        new-constraints
        (filter identity
                (for [[constraint & specs] add-constraints]
                  (if (or (not *db*)
                          (not (foreign-key-exists? *db* (to-sql-name constraint)))
                          (some (fn [prior]
                                  ;; Did we previously drop the constraint?
                                  (and (= table (:alter-table prior))
                                       (some (fn [cons] (= cons constraint))
                                             (:drop-constraints prior))))
                                @*plan*))
                    (do
                      (log/info "    * Adding constraint " (log/highlight (if constraint constraint "unnamed")))
                      (format "CONSTRAINT %s FOREIGN KEY %s" 
                              (to-sql-name constraint)
                              (s/join  " " specs)))
                    (log/info "    * Skipping adding constraint " (log/highlight (if constraint constraint "unnamed"))
                              " because it already exists"))))
        old-constraints
        (filter identity
                (for [constraint drop-constraints]
                  ;; As with drop-column, we are not checking here for add constraint followed by drop constraint
                  ;; in the same migration.  Wouldn't make sense.
                  (if (= constraint :primary-key)
                    (if-let [primary-key (get-primary-key *db* (to-sql-name table :quote-for-statement false))]
                      (do (log/info (format "   * Removing primary key %s from %s" primary-key table))
                          (to-sql-name primary-key)) 
                      )
                    (if (or (not *db*)
                            (foreign-key-exists? *db* (to-sql-name constraint)))
                      (do
                        (log/info "    * Removing constraint " (log/highlight constraint))
                        (to-sql-name constraint))
                      (log/info "   * Skipping removing constraint " (log/highlight constraint) " because it does not exist")))))
        options
        (when (not-empty table-options)
          (let [opts (generate-options table-options)]
            (when (not-empty opts)
              [opts])))
        ]
    (let [drops (s/join ", " 
                        (filter identity
                                [(if (not-empty old-constraints )
                                   (str "constraint " (s/join "," old-constraints))
                                   nil
                                   ) 
                                 (if (not-empty removals)
                                   (str "column " (s/join "," removals))
                                   nil)])) 
          adds (s/join ", "
                       (filter identity
                               [(if (not-empty additions)
                                  (s/join "," additions)
                                  nil) 
                                (if (not-empty new-constraints)
                                  (s/join "," new-constraints)
                                  nil
                                  )]))
          other-alterations (concat options modification primary-key-constraints)]
      (concat
       (if (not-empty drop-indices)
         [(format "drop index if exists %s" (s/join "," (map (fn [idx] 
                                                               (format "%s on %s"
                                                                       (to-sql-name idx)
                                                                       (to-sql-name table)))
                                                             drop-indices)))])
       (mapcat (fn [column]
                 (generate-sql {:alter-table table :modify-column column}))
               modify-columns)
       (if (not-empty drops)
         [(format "ALTER TABLE %s DROP %s" (to-sql-name table) drops)]
         nil
         )
       (if (not-empty adds)
         [(format "ALTER TABLE %s ADD %s" (to-sql-name table) adds)]
         nil
         )
       (if (not-empty other-alterations)
         [(format "ALTER TABLE %s %s"
                  (to-sql-name table)
                  (s/join ", " other-alterations))] 
         nil))
      )
    ))

(defmethod generate-sql
  :create-index
  [{index   :create-index
    table   :on
    columns :columns
    unique  :unique}]
  (if (and *db*
           (index-exists? *db* (to-sql-name table) (to-sql-name index))
           (not (some (fn [prior]
                        (and (= index (:drop-index prior))
                             (= table (:on prior))))
                      @*plan*)))
    (do (log/info (format "   * Skipping CREATE INDEX on table %s name %s because it already exists."
                          (to-sql-name table) (to-sql-name index)))
        nil)
    (do (log/info " - Creating" (if unique "unique" "") "index" (log/highlight (name index)) "on" (log/highlight (name table)))
        [(format "CREATE %s INDEX %s ON %s (%s)"
                 (if unique "UNIQUE" "")
                 (to-sql-name index)
                 (to-sql-name table)
                 (s/join ", " (map to-sql-name 
                                   (map (fn [column-name]
                                          (second (re-matches #"([^()]+)(?:\(\d+\))?" (name column-name))))
                                        columns))))])))

(defmethod generate-sql
  :drop-index
  [{index :drop-index table :on}]
  ;; As with drop-column, we are not checking here for create index followed by drop index
  ;; in the same migration.  Wouldn't make sense.
  (if (or (not *db*) (index-exists? *db* (to-sql-name table) (to-sql-name index)))
    (do (log/info " - Dropping index" (log/highlight (name index)))
        [(format "DROP INDEX %s ON %s" (to-sql-name index) (to-sql-name table))])
    (do (log/info (format "   * Skipping DROP INDEX on table %s index %s because that index does not exist."
                          table index))
        nil)))

(defmulti to-sql-val (fn [val] (type val)))
(defmethod to-sql-val :default [val] (pr-str val))
(defmethod to-sql-val String [val] (str "'" val "'"))


(defmethod generate-sql
  :insert-into
  [{table :insert-into values :values query :query columns :columns}]
  (log/info " - Inserting into" (log/highlight (name table)))
  (cond (not-empty values)
        [(format "INSERT INTO %s(%s) VALUES %s" (to-sql-name table)
                 (s/join "," columns)
                 (s/join "," (map (fn [vals] (format "(%s)" (s/join "," (map to-sql-val vals)))) values)))]
        (not-empty query)
        [(format "INSERT INTO %s %s" (to-sql-name table) query)]
        :else (throw (Exception. ":insert-into must contain a non-empty :values or :query key"))))

(defmethod generate-sql
  :update
  [{update-query :update}]
  (log/info " - Updating the database" (log/highlight update-query))
  (cond (not-empty update-query)
        [update-query]
        :else (throw (Exception. ":update must contain a non-empty update query"))))

(defmethod generate-sql
  :default
  [command]
  (log/error "Don't know how to process command:" (log/highlight (pr-str command)))
  (System/exit 1))

(defn- database-exists?
  [db]
  (jdbc/query
    (db-spec db)
    ["SELECT 1 FROM sys.databases WHERE NAME = ?" (:database db)]
    :result-set-fn first))

(defn get-primary-key
  [db table]
  (if db
    (:constraint_name 
     (jdbc/query
      (db-spec db)
      ["SELECT constraint_name from information_schema.table_constraints WHERE table_catalog=? AND TABLE_NAME=? AND CONSTRAINT_TYPE='PRIMARY KEY'" (:database db) table]
      :result-set-fn first)) 
    false)  
  )

(defn- primary-key-exists?
  [db table]
  (if db
    (jdbc/query
     (db-spec db)
     ["SELECT 1 from information_schema.table_constraints WHERE table_catalog=? AND TABLE_NAME=? AND CONSTRAINT_TYPE='PRIMARY KEY'" (:database db) table]
     :result-set-fn first)
    false))
  
(defn- foreign-key-exists?
  [db fk]
  (if db
    (jdbc/query
     (db-spec db)
     ["SELECT 1 from information_schema.table_constraints WHERE TABLE_CATALOG=? AND CONSTRAINT_NAME=? AND CONSTRAINT_TYPE='FOREIGN KEY'" (:database db) fk]
     :result-set-fn first)
    false))

(defn- index-exists?
  [db table index]
  (if db
    (do 
      ;;(log/info (format "   * Testing whether table %s has index %s" table index))
      (jdbc/query
       (db-spec db)
       ["SELECT 1 where indexproperty(object_id(?),?,'IndexID') is not null" (str (:database- db) "." table) index]
       :result-set-fn first))
    false))

(defn- table-exists?
  [db table]
  (jdbc/query
    (db-spec db)
    ["SELECT 1 FROM information_schema.tables WHERE table_catalog=? and table_name = ?" (:database db) table]
    :result-set-fn first))

(defn- column-exists?
  [db table column]
  (jdbc/query
   (db-spec db)
   ["SELECT 1 from information_schema.columns where table_catalog=? and table_name=? and column_name=?" (:database db) table column]
   :result-set-fn first))

(defn- create-database
  [db]
  (jdbc/db-do-commands
    (db-spec db) false
    (format "CREATE DATABASE %s" (:database db))))

(defn- create-version-table
  [db version-table]
  (apply jdbc/db-do-commands
         (db-spec db)
         (generate-sql
           {:create-table version-table
            :columns [[:version "VARCHAR(14)" "NOT NULL"]
                      [:created "datetime"      "NOT NULL"]]})))

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
      [(format "SELECT TOP 1 version FROM %s
                  ORDER BY version DESC"
               version-table)]
      :row-fn :version
      :result-set-fn first)))

(defn- update-schema-version
  [version-table version]
  (format "INSERT INTO %s (version,created) VALUES (%s,CURRENT_TIMESTAMP)"
          version-table version))

(defn- run-migration!
  [{:keys [db version-table]} version commands]
  (when-not (vector? commands)
    (log/error "Values for `up` and `down` must be vectors of commands"))
  (try
    (binding [*db* db
              *plan* (atom [])]
    (let [sql-commands (into [] (mapcat
                                 (fn [command]
                                   (let [sql (or (:sqlserver command)
                                                 (generate-sql command))]
                                     (swap! *plan* concat [command])
                                     (if (string? sql) [sql] sql)))
                                 commands))]
      (doseq [cmd sql-commands]
        (log/info "Executing: " (pr-str  cmd))
        (jdbc/db-do-commands (db-spec db) cmd))
      (log/info "Updating version to: " version)
      (jdbc/db-do-commands (db-spec db) (update-schema-version version-table version))))))

(defn- validate-commands
  [commands]
  (cond (and (vector? commands)
             (every? map? commands)) commands
        (map? commands) [vec commands]
        nil nil
        :else (do
                (log/error "The values for `up` and `down` must be either a map or a vector of maps.")
                (System/exit 1))))

(defrecord SqlServerMigrator
  [config]
  Migrator
  (init[this]
    (init-db! config))
  (version [this]
    (or (get-version config) "0"))
  (migrate [this version commands]
    (run-migration! config version (validate-commands commands))))
