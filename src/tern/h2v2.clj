(ns tern.h2v2
  (:require [tern.db           :refer :all]
            [tern.log          :as log]
            [clojure.java.jdbc :as jdbc]
            [clojure.string    :as s]
            [clojure.set       :as set])
  (:import [java.util Date]
           [java.sql PreparedStatement Timestamp]))

(declare foreign-key-exists? primary-key-exists? get-matching-foreign-keys index-exists? column-exists? table-exists? get-column-definition not-indexable-type?)

(def ^:dynamic *db* nil)
(def ^:dynamic *plan* nil)

(def ^{:doc "Set of supported commands. Used in `generate-sql` dispatch."
       :private true}
  supported-commands
  #{:create-table :drop-table :alter-table :create-index :drop-index :insert-into :update})

(defn remove-column-length
  "Sanitize a column name that might include a length, as these are not supported in H2"
  [column-name]
  (let [[_ name length rest] (re-matches #"(\w+)(:?\(\d+\))?(.*)" (clojure.core/name column-name))]
    (if (and name length) 
      (cond (= (s/upper-case name) "BINARY")
            column-name
            (not-empty  rest) 
            (str name " " rest)
            :else name)   
      column-name)))

(defn ^:private unsupported-column-spec
  [spec]
  (let [s (cond (string? spec)
                spec
                (keyword? spec)
                (name spec)
                (symbol? spec)
                (name spec)
                :else (str spec))]
    (or (re-matches #"(?i)CHARACTER SET .+" s)
        (re-matches #"(?i)COLLATE .+" s))))

(def ^:private column-spec-map
  {"DEFAULT NULL" "NULL"})

(def ^:private reserved-names
  #{"VALUE" "USER"})

(defn ^:private quote-reserved-name
  [name]
  (if (reserved-names (s/upper-case name))
    (str "`" name "`")
    name))

(defn to-h2-name
  [name]
  (-> (tern.db/to-sql-name name)
      s/upper-case
      quote-reserved-name))

(defn ^:private remove-unsupported-column-specs
  [specs]
  (if (and (symbol? (first specs))
           (list? (second specs))
           (= 1 (count (second specs)))
           (integer? (first (second specs))))
    ;; when the edn file has INT(11) rather than "INT(11)" we get a symbol followed by a list
    (remove-unsupported-column-specs
     (concat [(format "%s%s" (first specs) (second specs))]
             (drop 2 specs)))
    (concat [(remove-column-length (first specs))]
            (for [spec (rest specs) :when (not (unsupported-column-spec spec))]
              (get column-spec-map spec spec)))))
   
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
                      (to-h2-name constraint)
                      (s/join " " specs)))))]
    (when fks (map (fn [fk] [fk]) fks))))


(defn generate-options
  [table-options]
  (when (not-empty table-options)
    (s/join ", "
            (for [option table-options]
              (format "%s=%s" (:name option) (:value option))))))

(defmulti generate-sql
  (fn [c] (some supported-commands (keys c))))

(defmethod generate-sql
  :create-table
  [{table :create-table columns :columns table-options :table-options :as command}]
  (log/info " - Creating table" (log/highlight (name table)))
  (let [constraints (generate-constraints command)
        table-spec (generate-options table-options)
        pk (generate-pk command)]
    (if (and *db*
             (table-exists? *db* (to-h2-name table))
             (not (some (fn [prior] (= prior {:drop-table table})) @*plan*)))
      (do (log/info (format "  * Skipping CREATE TABLE %s because it already exists." (to-h2-name table)))
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
                [(format "ALTER TABLE %s ADD %s" (to-h2-name table) pk)]
                (generate-sql {:alter-table table
                               :drop-columns [:__placeholder]})))
        (let [sanitized-columns
              (for [[name & opts] columns]
                (cons (to-h2-name name) 
                      (remove-unsupported-column-specs 
                       (filter (comp not unsupported-column-spec) opts))))]
          [(if pk
             (apply jdbc/create-table-ddl table (apply conj sanitized-columns [pk] constraints))
             (if constraints
               (apply jdbc/create-table-ddl table (apply conj sanitized-columns constraints))
               (apply jdbc/create-table-ddl table sanitized-columns)))])))))

(defmethod generate-sql
  :drop-table
  [{table :drop-table}]
  (log/info " - Dropping table" (log/highlight (name table)))
  [(jdbc/drop-table-ddl table)])

(defn cleanup-constraints
  [table added-constraints]
  )

(defmethod generate-sql
  :alter-table
  [{table :alter-table add-columns :add-columns drop-columns :drop-columns modify-columns :modify-columns
    add-constraints :add-constraints  drop-constraints :drop-constraints primary-key :primary-key
    table-options :table-options character-set :character-set}]
  (log/info " - Altering table" (log/highlight (name table)))
  (let [additions
        (when (not-empty add-columns)
          [(format "alter table %s add column (%s)"
                   (to-h2-name table)
                   (s/join ", "
                           (filter identity
                                   (for [[column & specs] add-columns]
                                     (if (and *db*
                                              (column-exists? *db* (to-h2-name table) (to-sql-name column))
                                              (not (some (fn [prior]
                                                           (and (= table (:alter-table prior))
                                                                (some (fn [col] (= col column)) (:drop-columns prior))))
                                                         @*plan*)))
                                       (do (log/info (format "   * Skipping ADD COLUMN %s.%s because it already exists."
                                                             (to-h2-name table) (to-sql-name column)))
                                           nil)
                                       (do (log/info "    * Adding column" (log/highlight (name column)))
                                           (format "%s %s"
                                                   (to-h2-name column)
                                                   (s/join " " (remove-unsupported-column-specs specs)))))))))])
        removals (when (not-empty drop-columns)
                   [(format "alter table %s drop column %s"
                         (to-h2-name table)
                         (s/join ", "
                                 (filter identity
                                         (for [column drop-columns]
                                           ;; Note: No test for prior alter table/add column or create table w/ column
                                           ;; because frankly it would not make sense in a single migration to create
                                           ;; a table column and then immediately drop it.
                                           (if (or (not *db*)
                                                   (column-exists? *db* (to-h2-name table) (to-sql-name column)))
                                             (do (log/info "    * Dropping column" (log/highlight (name column)))
                                                 (format "%s" (to-h2-name column)))
                                             (do (log/info "   * Skipping DROP COLUMN %s.%s because that column does not exist."
                                                           (to-h2-name table) (to-h2-name column))
                                                 nil))))))])
        modifications (filter identity (for [[column & specs] modify-columns]
                                         (do (log/info "    * Modifying column" (log/highlight (name column)))
                                             (format "ALTER TABLE %s ALTER COLUMN %s %s"
                                                     (to-h2-name table)
                                                     (to-h2-name column)
                                                     (s/join " " (remove-unsupported-column-specs specs))))))
        primary-key-constraints
        (if primary-key
          (if (not (primary-key-exists? *db* (to-h2-name table)))
            (do (log/info "   * Adding primary key to table " (log/highlight table))
                [(format "ALTER TABLE %S ADD PRIMARY KEY %s"
                         (to-h2-name table)
                         (generate-pk {:primary-key primary-key}))])
            (do (log/info "   * Not adding primary key to " (log/highlight table) " because it already exists")
                []))
          [])
        new-constraints (filter
                         identity
                         (mapcat
                          (fn [[constraint & specs]]
                            (let [spec-sql (s/join " " specs)
                                  [_ fkcolumn pktable pkcolumn] (re-matches #"(?i)\(([\w_]+)\)\s+REFERENCES\s+([\w_]+)\(([\w_]+)\).*" spec-sql)
                                  _ (if (or (nil? fkcolumn) (nil? pktable) (nil? pkcolumn))
                                      (log/error "Failed to parse constraint for" constraint ":" spec-sql))
                                  foreign-keys (get-matching-foreign-keys *db* (to-h2-name table) (to-h2-name fkcolumn)
                                                                          (to-h2-name pktable) (to-h2-name pkcolumn))
                                  existing-constraints (map (comp s/upper-case to-h2-name :constraint_name) foreign-keys)
                                  duplicate-constraints (disj (set existing-constraints) (s/upper-case (to-h2-name constraint)))
                                  undropped-existing-constraints (set/union 
                                                                  (map (comp s/upper-case name) duplicate-constraints) 
                                                                  (set (filter 
                                                                        (fn [existing]
                                                                          (not 
                                                                           (some 
                                                                            (fn [prior]
                                                                              (some (fn [to-drop]
                                                                                      (= (s/upper-case (to-h2-name to-drop)) existing))
                                                                                    (:drop-constraints prior)))
                                                                                     @*plan*)))
                                                                        existing-constraints)))]
                              (if (or (not *db*)
                                      (not (foreign-key-exists? *db* (to-h2-name constraint)))
                                      (some (fn [prior]
                                              ;; Did we previously drop the constraint?
                                              (and (= table (:alter-table prior))
                                                   (some (fn [cons] (= cons constraint))
                                                         (:drop-constraints prior))))
                                            @*plan*))
                                (do
                                  (log/info "    * Adding constraint " (log/highlight (if constraint constraint "unnamed")))
                                  (concat (into []
                                                (for [undropped-constraint (set/difference 
                                                                            duplicate-constraints
                                                                            (set (map (comp s/upper-case to-h2-name name) drop-constraints)))  
                                                      #_(distinct undropped-existing-constraints)]
                                                  (do (log/info "    * Dropping foreign key" undropped-constraint "because it is being replaced")
                                                      (format "ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s"
                                                              (to-h2-name table)
                                                              (s/upper-case (to-h2-name undropped-constraint))))))
                                          [(format "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY %s" 
                                                   (to-h2-name table)
                                                   (to-h2-name constraint)
                                                   spec-sql)]))
                                (do (log/info "    * Skipping adding constraint " (log/highlight (if constraint constraint "unnamed"))
                                              " because it already exists")
                                    nil))))
                          add-constraints))
        old-constraints (distinct 
                         (filter
                          identity
                          (for [constraint drop-constraints]
                            ;; As with drop-column, we are not checking here for add constraint followed by drop constraint
                            ;; in the same migration.  Wouldn't make sense.
                            (if (= constraint :primary-key)
                              (if (or (not *db*)
                                      (primary-key-exists? *db* (to-h2-name table)))
                                (do
                                  (log/info "    * Removing primary key from " (log/highlight table))
                                  (format "ALTER TABLE %s DROP PRIMARY KEY",
                                          (to-h2-name table)))
                                (log/info "   * Skipping removing primary key from " (log/highlight table) " because it does not have one"))
                              (if (or (not *db*)
                                      (foreign-key-exists? *db* (to-h2-name constraint)))
                                (do
                                  (log/info "    * Removing constraint " (log/highlight constraint))
                                  (format "ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s",
                                          (to-h2-name table)
                                          (to-h2-name constraint)))
                                (log/info "   * Skipping removing constraint " (log/highlight constraint) " because it does not exist")))))) 
        options
        (when (not-empty table-options)
          (log/info "   * Skipping options " table-options " because H2 does not support them")
          [])
        charset
        (when character-set
          (log/info "   * Skipping convert to character set because H2 does not support it.")
          [])]
    (doall (concat options charset old-constraints removals additions modifications primary-key-constraints new-constraints))))


(defmethod generate-sql
  :drop-index
  [{index :drop-index table :on}]
  ;; As with drop-column, we are not checking here for create index followed by drop index
  ;; in the same migration.  Wouldn't make sense.
  (if (or (not *db*) (index-exists? *db* (to-h2-name table) (to-h2-name index)))
    (do (log/info " - Dropping index" (log/highlight (name index)))
        [(format "DROP INDEX %s ON %s" (to-h2-name index) (to-h2-name table))])
    (do (log/info (format "   * Skipping DROP INDEX on table %s index %s because that index does not exist."
                          table index))
        nil)))

(defmethod generate-sql
  :insert-into
  [{table :insert-into values :values query :query}]
  (log/info " - Inserting into" (log/highlight (name table)))
  (cond (not-empty values)
        [(format "INSERT INTO %s VALUES %s" (to-h2-name table)
                 (s/join "," (map (fn [vals] (format "(%s)" (s/join ","
                                                                    (map #(if (string? %)
                                                                            (str "'" % "'")
                                                                            (pr-str %))
                                                                         vals))))
                                  values)))]
        (not-empty query)
        [(format "INSERT INTO %s %s" (to-h2-name table) query)]
        :else (throw (Exception. ":insert-into must contain a non-empty :values or :query key"))))

(defmethod generate-sql
  :update
  [{update-query :update h2-version :h2}]
  (log/info " - Updating the database" (log/highlight update-query))
  (let [command (or (not-empty h2-version) (not-empty update-query))]
    (cond (not-empty command)
          [command]
          :else (throw (Exception. ":update must contain a non-empty update statement or :h2 version of the same statement")))))

(defmethod generate-sql
  :default
  [command]
  (log/error "Don't know how to process command:" (log/highlight (pr-str command)))
  (System/exit 1))

(defn- database-exists?
  [db]
  (jdbc/query
   (db-spec db)
   ["SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?" (:schema db "PUBLIC")]
   :result-set-fn first))





(defmethod generate-sql
  :create-index
  [{index   :create-index
    table   :on
    columns :columns
    unique  :unique}]
  (if (and *db*
           (index-exists? *db* (to-h2-name table) (to-h2-name index))
           (not (some (fn [prior]
                        (and (= index (:drop-index prior))
                             (= table (:on prior))))
                      @*plan*)))
    (do (log/info (format "   * Skipping CREATE INDEX on table %s name %s because it already exists."
                          (to-h2-name table) (to-h2-name index)))
        nil)
    (let [columns (map (comp remove-column-length to-h2-name) columns)
          indexable-columns (filter (fn [column]
                                      (let [type (or (:data_type (get-column-definition *db* (to-h2-name table) column))
                                                     (some (fn [prior]
                                                             (cond (= table (:create-table prior))
                                                                   (some (fn [col]
                                                                           (and (= (to-h2-name (first col)) column)
                                                                                (second col)))
                                                                         (:columns prior))
                                                                   (= table (:alter-table prior))
                                                                   (some (fn [col]
                                                                           (and (= (to-h2-name (first col)) column)
                                                                                (second col)))
                                                                         (:add-columns prior))))
                                                           @*plan*))]
                                        (not (not-indexable-type? type))))
                                    columns)]
      (if (empty? indexable-columns)
        (do (log/warn (format "    * Skipping creating index %s because none of the specified columns are indexable." index))
            [])
        (do (log/info " - Creating" (if unique "unique" "") "index" (log/highlight (name index)) "on" (log/highlight (name table)))
            [(format "CREATE %s INDEX %s ON %s (%s)"
                     (if unique "UNIQUE" "")
                     (to-h2-name index)
                     (to-h2-name table)
                     (s/join ", " indexable-columns))])))))

(defn get-matching-foreign-keys
  [db fktable fkcolumn pktable pkcolumn]
  (let [foreign-key-constraints 
        (map :constraint_name
             (jdbc/query 
              (db-spec db)
              [(str "select constraint_name from information_schema.table_constraints "
                    "where table_name=? and constraint_type='FOREIGN KEY' and constraint_schema=SCHEMA()") 
               (s/upper-case fktable )]))
        primary-key-constraints
        (for [fk foreign-key-constraints]
          (:unique_constraint_name
           (jdbc/query
            (db-spec db)
            [(str "select unique_constraint_name from information_schema.referential_constraints "
                  "where constraint_name=? and constraint_schema=SCHEMA()")
             fk]
            :result-set-fn first))) 
        constraints (map (fn [fk pk]
                           (let [fkcol (jdbc/query 
                                        (db-spec db)
                                        [(str "select table_name,column_name,constraint_name from information_schema.constraint_column_usage "
                                              "where table_name=? and constraint_name=? and constraint_schema=SCHEMA()")
                                         fktable fk]
                                        :result-set-fn first)
                                 pkcol (jdbc/query 
                                        (db-spec db)
                                        [(str "select table_name,column_name from information_schema.constraint_column_usage "
                                              "where constraint_name=? and constraint_schema=SCHEMA()")
                                         pk]
                                        :result-set-fn first)]
                             {:constraint_name (:constraint_name fkcol)
                              :fktable (:table_name fkcol) :fkcolumn (:column_name fkcol)
                              :pktable (:table_name pkcol) :pkcolumn (:column_name pkcol)}))
                         foreign-key-constraints primary-key-constraints)]
    (->> constraints
         (filter (fn [constraint]
                   (and (= (s/upper-case fktable) (:fktable constraint))
                        (= (s/upper-case fkcolumn) (:fkcolumn constraint))
                        (= (s/upper-case pktable) (:pktable constraint))
                        (= (s/upper-case pkcolumn) (:pkcolumn constraint))))))))

(defn primary-key-exists?
  [db table]
  (if db
    (jdbc/query
     (db-spec db)
     ["SELECT 1 from information_schema.table_constraints WHERE CONSTRAINT_SCHEMA=SCHEMA() AND TABLE_NAME=? AND CONSTRAINT_TYPE='PRIMARY KEY'"
      (s/upper-case table)]
     :result-set-fn first)
    false))

(defn foreign-key-exists?
  [db fk]
  (if db
    (jdbc/query
     (db-spec db)
     ["SELECT 1 from information_schema.table_constraints WHERE CONSTRAINT_SCHEMA=SCHEMA() AND CONSTRAINT_NAME=? AND CONSTRAINT_TYPE='FOREIGN KEY'"
      (s/upper-case fk)]
     :result-set-fn first)
    false))

(defn- index-exists?
  [db table index]
  (if db
    (do 
      ;;(log/info (format "   * Testing whether table %s has index %s" table index))
      (jdbc/query
       (db-spec db)
       ["SELECT 1 from information_schema.indexes WHERE TABLE_SCHEMA=SCHEMA() AND TABLE_NAME=? AND INDEX_NAME=?"
        (s/upper-case table)
        (s/upper-case index)]
       :result-set-fn first))
    false))

(defn- table-exists?
  [db table]
  (jdbc/query
    (db-spec db)
    ["SELECT 1 FROM information_schema.tables WHERE table_schema=SCHEMA() and table_name = ?" (s/upper-case table)]
    :result-set-fn first))

(defn not-indexable-type?
  [datatype]
  (#{"CLOB"
     "NCLOB" "BLOB" "TINYBLOB" "MEDIUMBLOB" "LONGBLOB" "IMAGE" "OID"
     ;; yeah, these are not indexable either--even though you'll find suggestions on stackoverflow that they are.
     "TINYTEXT" "TEXT" "MEDIUMTEXT" "LONGTEXT" "NTEXT" "CHARACTER LARGE OBJECT"
     }
   (s/upper-case (name datatype))))

(defn- get-column-definition
  [db table column]
  (jdbc/query
   (db-spec db)
   ["SELECT * from information_schema.columns where table_schema=SCHEMA() and table_name=? and column_name=?"
    (s/upper-case table)
    (s/upper-case column)
    ]
   :result-set-fn first
   ))

(defn- column-exists?
  [db table column]
  (jdbc/query
   (db-spec db)
   ["SELECT 1 from information_schema.columns where table_schema=SCHEMA() and table_name=? and column_name=?"
    (s/upper-case table)
    (s/upper-case column)]
   :result-set-fn first))

(defn- create-database
  [db]
  (jdbc/db-do-commands
    (db-spec db) false
    (format "CREATE SCHEMA %s" (:schema db "PUBLIC"))))

(defn- create-version-table
  [db version-table]
  (apply jdbc/db-do-commands
         (db-spec db)
         (generate-sql
           {:create-table version-table
            :columns [[:version "VARCHAR(14)" "NOT NULL"]
                      [:created "timestamp"      "NOT NULL" "default CURRENT_TIMESTAMP"]]})))

(defn- psql-error-message
  [e]
  (s/replace (.getMessage e) #"^(FATAL|ERROR): " ""))

(defn- batch-update-error-message
  [e]
  (s/replace (.getMessage (.getNextException e)) #"^(FATAL|ERROR): " ""))

(defn- init-db!
  [{:keys [db version-table]}]
  (when-not (database-exists? db)
    (create-database db)
    (log/info "Created database:" (:schema db "PUBLIC")))
  (when-not (table-exists? db version-table)
    (create-version-table db version-table)
    (log/info "Created table:   " version-table)))

(defn- get-version
  [{:keys [db version-table]}]
  (let [dbspec (db-spec db)
        connection (jdbc/get-connection dbspec)]
    (try
      (jdbc/query
       dbspec
       [(format "SELECT version FROM %s
                  ORDER BY created DESC, version DESC
                  LIMIT 1" version-table)]
       :row-fn :version
       :result-set-fn first))))

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
      #_(doseq [column (jdbc/query (db-spec db)
                                 ["select * from information_schema.columns where table_schema=SCHEMA() and table_name=? and column_name=?"
                                  "TAXNODES" "PATH"])]
          (println (pr-str column)))
      (let [sql-commands (into [] (mapcat
                                   (fn [command]
                                     (let [sql (generate-sql command)]
                                       (swap! *plan* concat [command])
                                       sql))
                                   commands))]
        (doseq [cmd sql-commands]
          (log/info "Executing: " cmd)
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

(defrecord H2V2Migrator
  [config]
  Migrator
  (init[this]
    (init-db! config))
  (version [this]
    (or (get-version config) "0"))
  (migrate [this version commands]
    (log/info "This is the H2 migrator for version 2.x.y of H2")
    (run-migration! config version (validate-commands commands))))
