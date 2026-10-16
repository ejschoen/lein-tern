(ns tern.sqlserver
  (:use [tern.db :exclude [to-sql-name]])
  (:require [tern.log          :as log]
            [clojure.java.jdbc :as jdbc]
            [clojure.string    :as s])
  (:import [java.util Date]
           [java.sql Connection PreparedStatement Statement Timestamp]))

(declare primary-key-exists? get-primary-key foreign-key-exists? index-exists? column-exists? table-exists? get-column-constraints
         get-column-indexes get-column-defaults folded-name plan-dropped-tables)

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
   "default false" "default 'false'"
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
             (not (contains? (plan-dropped-tables @*plan*) (folded-name table))))
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

(defn- catalog-name
  "A name as the SQL Server catalog holds it: to-sql-name without statement quoting."
  [k]
  (to-sql-name k :quote-for-statement false))

(defn- folded-name
  "A name as the plan and the catalog are compared: its catalog name in lower
  case.  SQL Server's default collation does not tell identifiers apart by case,
  and neither do the OBJECT_ID and indexproperty the guards ask, so :IX_A and
  ix_a name one index."
  [k]
  (s/lower-case (catalog-name k)))

(def ^:private drop-index-statement
  "One whole T-SQL statement `DROP INDEX [IF EXISTS] <index> ON <table>`: keywords
  in any case, any whitespace, each name bare, [bracketed] or \"double-quoted\",
  the table possibly qualified (only its last segment is captured), an optional
  trailing semicolon.  Group 1 is the index, group 2 the table."
  #"(?i)\s*DROP\s+INDEX\s+(?:IF\s+EXISTS\s+)?(\[[^\]]+\]|\"[^\"]+\"|[^\s\[\]\".;,]+)\s+ON\s+(?:(?:\[[^\]]*\]|\"[^\"]*\"|[^\s\[\]\".;,]*)\s*\.\s*)*(\[[^\]]+\]|\"[^\"]+\"|[^\s\[\]\".;,]+)\s*;?\s*")

(def ^:private drop-table-statement
  "One whole T-SQL statement `DROP TABLE [IF EXISTS] <table>`, read as
  drop-index-statement reads its statement (a bare name also ends at a
  parenthesis).  Group 1 is the table."
  #"(?i)\s*DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:(?:\[[^\]]*\]|\"[^\"]*\"|[^\s\[\]\".;,()]*)\s*\.\s*)*(\[[^\]]+\]|\"[^\"]+\"|[^\s\[\]\".;,()]+)\s*;?\s*")

(def ^:private drop-constraint-statement
  "One whole T-SQL statement `ALTER TABLE <table> DROP CONSTRAINT [IF EXISTS]
  <constraint>[, <constraint> ...]`, read as drop-table-statement reads its
  statement.  Group 1 is the table, group 2 the constraints (sql-identifier finds
  each)."
  #"(?i)\s*ALTER\s+TABLE\s+(?:(?:\[[^\]]*\]|\"[^\"]*\"|[^\s\[\]\".;,()]*)\s*\.\s*)*(\[[^\]]+\]|\"[^\"]+\"|[^\s\[\]\".;,()]+)\s+DROP\s+CONSTRAINT\s+(?:IF\s+EXISTS\s+)?((?:\[[^\]]+\]|\"[^\"]+\"|[^\s\[\]\".;,()]+)(?:\s*,\s*(?:\[[^\]]+\]|\"[^\"]+\"|[^\s\[\]\".;,()]+))*)\s*;?\s*")

(def ^:private create-index-statement
  "One whole T-SQL statement `CREATE [UNIQUE] [CLUSTERED|NONCLUSTERED] INDEX
  <index> ON <table> (<columns>) [INCLUDE (<columns>)]`, read as
  drop-table-statement reads its statement; a key column may be followed by ASC
  or DESC.  Group 1 is the index, group 2 the table, group 3 the key columns
  (index-key-column finds each), group 4 the included columns, if any
  (sql-identifier finds each)."
  #"(?i)\s*CREATE\s+(?:UNIQUE\s+)?(?:(?:CLUSTERED|NONCLUSTERED)\s+)?INDEX\s+(\[[^\]]+\]|\"[^\"]+\"|[^\s\[\]\".;,()]+)\s+ON\s+(?:(?:\[[^\]]*\]|\"[^\"]*\"|[^\s\[\]\".;,()]*)\s*\.\s*)*(\[[^\]]+\]|\"[^\"]+\"|[^\s\[\]\".;,()]+)\s*\(\s*((?:\[[^\]]+\]|\"[^\"]+\"|[^\s\[\]\".;,()]+)(?:\s+(?:ASC|DESC))?(?:\s*,\s*(?:\[[^\]]+\]|\"[^\"]+\"|[^\s\[\]\".;,()]+)(?:\s+(?:ASC|DESC))?)*)\s*\)(?:\s*INCLUDE\s*\(\s*((?:\[[^\]]+\]|\"[^\"]+\"|[^\s\[\]\".;,()]+)(?:\s*,\s*(?:\[[^\]]+\]|\"[^\"]+\"|[^\s\[\]\".;,()]+))*)\s*\))?\s*;?\s*")

(def ^:private sql-identifier
  "One name of a list a statement pattern above has matched."
  #"\[[^\]]+\]|\"[^\"]+\"|[^\s\[\]\".;,()]+")

(def ^:private index-key-column
  "One key column of a list create-index-statement has matched, with its ASC or
  DESC.  Group 1 is the name."
  #"(?i)(\[[^\]]+\]|\"[^\"]+\"|[^\s\[\]\".;,()]+)(?:\s+(?:ASC|DESC))?")

(defn- unquoted-identifier
  "`s` without the [brackets] or \"double quotes\" that delimit it, if any."
  [s]
  (if (or (and (s/starts-with? s "[") (s/ends-with? s "]"))
          (and (s/starts-with? s "\"") (s/ends-with? s "\"")))
    (subs s 1 (dec (count s)))
    s))

(defn- override-statements
  "The statements of a `:sqlserver` override: each string in it, a string or a
  vector of strings at any depth, as run-migration! runs them
  (tern.db/sql-statements)."
  [override]
  (filter string? (tree-seq sequential? seq override)))

(defn- as-run
  "What `read-override` finds in the `:sqlserver` override of `command` when it
  has one, and otherwise what `read-command` finds in the command: run-migration!
  runs an override in place of the command's own keys, so those keys do nothing
  on SQL Server and are not read."
  [command read-override read-command]
  (if-let [override (:sqlserver command)]
    (read-override override)
    (read-command command)))

(defn- override-dropped-indexes
  "The [table index] pairs, folded (folded-name), that the statements of a
  `:sqlserver` override drop: each that is one DROP INDEX ... ON ... statement.
  Any other statement, or a DROP INDEX naming no table, drops nothing here."
  [override]
  (for [statement (override-statements override)
        :let [[_ index table] (re-matches drop-index-statement statement)]
        :when index]
    [(folded-name (unquoted-identifier table)) (folded-name (unquoted-identifier index))]))

(defn- plan-dropped-indexes
  "The [table index] pairs, folded (folded-name), that the commands in `plan`
  drop: a `{:drop-index i :on t}` command, or the `:drop-index` key of an
  `:alter-table` (one index, or a collection of them).  A `:drop-index` command
  without `:on` names no table and is left out.  A command with a `:sqlserver`
  override counts only by the DROP INDEX statements the override holds (as-run)."
  [plan]
  (set (mapcat (fn [command]
                 (as-run command
                         override-dropped-indexes
                         (fn [{index :drop-index table :alter-table on :on}]
                           (cond (nil? index) nil
                                 table        (for [i (if (coll? index) index [index])]
                                                [(folded-name table) (folded-name i)])
                                 on           [[(folded-name on) (folded-name index)]]))))
               plan)))

(defn- override-dropped-constraints
  "The [table constraint] pairs, folded (folded-name), that the statements of a
  `:sqlserver` override drop: each constraint of each statement that is one
  ALTER TABLE ... DROP CONSTRAINT ... statement."
  [override]
  (for [statement (override-statements override)
        :let [[_ table constraints] (re-matches drop-constraint-statement statement)]
        :when table
        constraint (re-seq sql-identifier constraints)]
    [(folded-name (unquoted-identifier table)) (folded-name (unquoted-identifier constraint))]))

(defn- plan-dropped-constraints
  "The [table constraint] pairs, folded (folded-name), that the commands in
  `plan` drop: the `:drop-constraints` key of an `:alter-table`, or, for a command
  with a `:sqlserver` override, only the ALTER TABLE ... DROP CONSTRAINT
  statements the override holds (as-run)."
  [plan]
  (set (mapcat (fn [command]
                 (as-run command
                         override-dropped-constraints
                         (fn [{table :alter-table constraints :drop-constraints}]
                           (when (and table constraints)
                             (for [constraint (if (coll? constraints) constraints [constraints])]
                               [(folded-name table) (folded-name constraint)])))))
               plan)))

(defn- override-dropped-tables
  "The tables, folded (folded-name), that the statements of a `:sqlserver`
  override drop: each that is one DROP TABLE statement."
  [override]
  (for [statement (override-statements override)
        :let [[_ table] (re-matches drop-table-statement statement)]
        :when table]
    (folded-name (unquoted-identifier table))))

(defn- plan-dropped-tables
  "The tables, folded (folded-name), that the commands in `plan` drop: a
  `{:drop-table t}` command, or, for a command with a `:sqlserver` override, only
  the DROP TABLE statements the override holds (as-run).  Dropping a table drops
  every index and constraint on it, so a later `:create-index` or
  `:add-constraints` on a table of this set is emitted even though the catalog,
  read before anything runs, still shows the old one."
  [plan]
  (set (mapcat (fn [command]
                 (as-run command
                         override-dropped-tables
                         (fn [{table :drop-table}]
                           (when table [(folded-name table)]))))
               plan)))

(defn- created-indexes
  "The indexes `command` creates (as-run), each [table index columns]: the table
  folded (folded-name), the index as a catalog name, the columns a set of folded
  names.  A `{:create-index i :on t :columns [...]}` creates i over its columns,
  each without the (n) prefix length the generator strips; a `:sqlserver`
  override creates what its CREATE INDEX statements create, over their key and
  included columns."
  [command]
  (as-run command
          (fn [override]
            (for [statement (override-statements override)
                  :let [[_ index table key-columns included] (re-matches create-index-statement statement)]
                  :when index]
              [(folded-name (unquoted-identifier table))
               (catalog-name (unquoted-identifier index))
               (set (for [column (concat (map second (re-seq index-key-column key-columns))
                                         (when included (re-seq sql-identifier included)))]
                      (folded-name (unquoted-identifier column))))]))
          (fn [{index :create-index table :on columns :columns}]
            (when (and index table)
              [[(folded-name table)
                (catalog-name index)
                (set (for [column columns]
                       (folded-name (second (re-matches #"([^()]+)(?:\(\d+\))?" (name column))))))]]))))

(defn- guarded-index-drop?
  "Whether `command` is a `{:drop-index i :on t}` that its generator emits only
  when the catalog, read before the migration runs, shows the index: it never
  drops an index an earlier command of the same migration creates."
  [command]
  (and (:drop-index command) (not (:alter-table command)) (not (:sqlserver command))))

(defn- plan-created-indexes
  "The indexes on `table` (folded) that the commands in `plan` create and no
  later command in `plan` drops, in plan order, each [index columns] as
  created-indexes gives them.  A later drop is a DROP INDEX of the index on the
  table in an override, the `:drop-index` key of an `:alter-table` of the table,
  or a drop of the whole table, but not a guarded-index-drop?."
  [plan table]
  (let [plan (vec plan)]
    (for [position         (range (count plan))
          [t index columns] (created-indexes (nth plan position))
          :when (= t table)
          :let [later (remove guarded-index-drop? (subvec plan (inc position)))]
          :when (not (or (contains? (plan-dropped-indexes later) [t (folded-name index)])
                         (contains? (plan-dropped-tables later) t)))]
      [index columns])))

(defn- distinct-folded
  "`names` in order, without a name that folds (folded-name) as an earlier one does."
  [names]
  (second (reduce (fn [[seen kept] n]
                    (if (seen (folded-name n))
                      [seen kept]
                      [(conj seen (folded-name n)) (conj kept n)]))
                  [#{} []]
                  names)))

(defn- exit!
  "Ends the process with `status`, as tern does when a migration cannot be run.
  A var, so that a test can trap the exit."
  [status]
  (System/exit status))

(defn- refuse-indexed-column-drops!
  "SQL Server refuses to drop a column that an index contains (error 5074).  When
  an :alter-table of `table` drops `columns` and an index containing one of them
  is still there when the command runs, this stops the migration before any of
  its statements runs: one error line per column and index, then the exit, as
  tern's other refusals do.  The indexes are those the catalog shows
  (get-column-indexes, read while the migration's SQL is generated) and, with a
  connection, those an earlier command of the migration creates
  (plan-created-indexes).  A catalog index counts as dropped first when an
  earlier {:drop-index i :on table} command or :alter-table :drop-index key, an
  earlier :sqlserver override's DROP INDEX ... ON table statement, or this
  command's own :drop-index key (`own-drops`) names it (plan-dropped-indexes), or
  when an earlier command drops the whole table (plan-dropped-tables); a created
  one when a command after its create, this command's own key included, drops it.
  Names are compared folded (folded-name).  Indexes that back a primary key or a
  unique constraint are not looked up here: they are constraints, dropped before
  the column with its other constraints."
  [table columns own-drops]
  (let [table-name (catalog-name table)
        table-key  (folded-name table)
        plan       (concat (if *plan* @*plan* []) [{:alter-table table :drop-index own-drops}])
        dropped    (plan-dropped-indexes plan)
        old-gone   (contains? (plan-dropped-tables plan) table-key)
        created    (when *db* (plan-created-indexes plan table-key))
        remaining  (doall (for [column columns
                                :let [column-key (folded-name column)
                                      found      (when-not old-gone
                                                   (remove #(contains? dropped [table-key (folded-name %)])
                                                           (get-column-indexes *db* table-name (catalog-name column))))]
                                index (distinct-folded (concat found (for [[index index-columns] created
                                                                           :when (contains? index-columns column-key)]
                                                                       index)))]
                            [(catalog-name column) index]))]
    (when (seq remaining)
      (doseq [[column index] remaining]
        (log/error (format (str "tern: cannot drop column %s.%s on SQL Server: index %s still contains it;"
                                " drop the index first in this migration (:drop-index), or remove a stale"
                                " index by hand (see the SQL Server migration runbook, section 5a)")
                           table-name column index)))
      (exit! 1))))

(defmethod generate-sql
  :alter-table
  [{table :alter-table add-columns :add-columns drop-columns :drop-columns modify-columns :modify-columns modify-column :modify-column
    add-constraints :add-constraints  drop-constraints :drop-constraints drop-indices :drop-index
    primary-key :primary-key
    table-options :table-options}]
  (log/info " - Altering table" (log/highlight (name table)))
  (let [;; :drop-index may name one index rather than a collection of them.
        drop-indices (if (or (nil? drop-indices) (coll? drop-indices)) drop-indices [drop-indices])
        additions
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
        removed-columns
        (filter identity
                (for [column drop-columns]
                  ;; Note: No test for prior alter table/add column or create table w/ column
                  ;; because frankly it would not make sense in a single migration to create
                  ;; a table column and then immediately drop it.
                  (if (or (not *db*)
                          (column-exists? *db* (to-sql-name table :quote-for-statement false) (to-sql-name column :quote-for-statement false)))
                    (do (log/info "    * Dropping column" (log/highlight (name column)))
                        column)
                    (do (log/info (format "   * Skipping DROP COLUMN %s.%s because that column does not exist."
                                          (to-sql-name table) (to-sql-name column)))
                        nil))))
        removals
        (map to-sql-name removed-columns)
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
          [(str "ADD " (generate-pk {:primary-key primary-key}))]
          [])
        new-constraints
        (filter identity
                (for [[constraint & specs] add-constraints]
                  (if (or (not *db*)
                          (not (foreign-key-exists? *db* (to-sql-name constraint)))
                          ;; Did an earlier command drop the constraint, or its table?
                          (contains? (plan-dropped-constraints @*plan*) [(folded-name table) (folded-name constraint)])
                          (contains? (plan-dropped-tables @*plan*) (folded-name table)))
                    (do
                      (log/info "    * Adding constraint " (log/highlight (if constraint constraint "unnamed")))
                      (format "CONSTRAINT %s FOREIGN KEY %s" 
                              (to-sql-name constraint)
                              (s/join  " " specs)))
                    (log/info "    * Skipping adding constraint " (log/highlight (if constraint constraint "unnamed"))
                              " because it already exists"))))
        old-check-constraints
        (doall (for [column drop-columns
                     constraint (get-column-constraints *db* (to-sql-name table :quote-for-statement false) (to-sql-name column :quote-for-statement false))]
                 (do (log/info "    * Removing constraint " (log/highlight constraint))
                     (to-sql-name constraint))))
        ;; An index that still contains a dropped column stops the migration here, while
        ;; its SQL is generated: the migration must drop the index first.
        _ (refuse-indexed-column-drops! table removed-columns drop-indices)
        ;; SQL Server also refuses to drop a column that a default constraint depends on.
        ;; A default constraint (sys.default_constraints: information_schema does not list
        ;; defaults) is dropped in the same ALTER TABLE statement as its column, which SQL
        ;; Server applies whole or not at all: a column drop that still fails leaves the
        ;; column with its default.
        old-defaults
        (doall (for [default (distinct
                              (for [column removed-columns
                                    default (get-column-defaults *db* (to-sql-name table :quote-for-statement false) (to-sql-name column :quote-for-statement false))]
                                default))]
                 (do (log/info "    * Removing default constraint " (log/highlight default))
                     (to-sql-name default))))
        old-constraints
        (filter #(and % (not ((set old-check-constraints) %)))
                (doall (for [constraint drop-constraints]
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
                             (log/info "   * Skipping removing constraint " (log/highlight constraint) " because it does not exist"))))))
        options
        (when (not-empty table-options)
          (let [opts (generate-options table-options)]
            (when (not-empty opts)
              [opts])))
        ]
    (let [check-drops
          (s/join ", " 
                  (filter identity
                          [(if (not-empty old-check-constraints )
                             (str "constraint if exists " (s/join "," old-check-constraints)) nil)]))
          table-constraints (concat old-constraints old-defaults)
          drops (s/join ", " 
                        (filter identity
                                [(if (not-empty table-constraints)
                                   (str "constraint if exists " (s/join "," table-constraints))
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
       (if (not-empty check-drops)
         [(format "ALTER TABLE %s DROP %s" (to-sql-name table) check-drops)]
         nil)
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
           (not (or (contains? (plan-dropped-indexes @*plan*) [(folded-name table) (folded-name index)])
                    (contains? (plan-dropped-tables @*plan*) (folded-name table)))))
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
      ;; OBJECT_ID('[database_name.[schema_name].|schema_name.]object_name'): a two-part
      ;; "database.table" is read as schema.table and resolves to NULL, so every index looked
      ;; absent.  "database..table" names the database and leaves the schema to the caller's default.
      (jdbc/query
       (db-spec db)
       ["SELECT 1 where indexproperty(object_id(?),?,'IndexID') is not null" (str (:database db) ".." table) index]
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
  (if db
    (jdbc/query
     (db-spec db)
     ["SELECT 1 from information_schema.columns where table_catalog=? and table_name=? and column_name=?" (:database db) table column]
     :result-set-fn first)
    false))

(defn- get-column-constraints
  [db table column]
  (if db
    (jdbc/query
     (db-spec db)
     ["select constraint_name from information_schema.constraint_column_usage where table_catalog=? and table_name=? and column_name=?"
      (:database db) table column]
     :row-fn :constraint_name)
    []))

(def ^:private column-indexes-sql
  (str "SELECT i.name FROM sys.indexes i"
       " JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id"
       " JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id"
       " JOIN sys.objects o ON o.object_id = i.object_id"
       " WHERE o.name = ? AND c.name = ? AND i.name IS NOT NULL"
       " AND i.is_primary_key = 0 AND i.is_unique_constraint = 0"))

(defn- get-column-indexes
  "Names of the indexes on `table` whose key or included columns contain
  `column`, from sys.indexes; [] without a connection.  Indexes that back a
  primary key or a unique constraint are left out: they are constraints, and
  get-column-constraints finds them."
  [db table column]
  (if db
    (jdbc/query
     (db-spec db)
     [column-indexes-sql table column]
     :row-fn :name)
    []))

(def ^:private column-defaults-sql
  (str "SELECT d.name FROM sys.default_constraints d"
       " JOIN sys.columns c ON c.object_id = d.parent_object_id AND c.column_id = d.parent_column_id"
       " JOIN sys.objects o ON o.object_id = d.parent_object_id"
       " WHERE o.name = ? AND c.name = ?"))

(defn- get-column-defaults
  "Names of the default constraints on `column` of `table`, from
  sys.default_constraints; [] without a connection."
  [db table column]
  (if db
    (jdbc/query
     (db-spec db)
     [column-defaults-sql table column]
     :row-fn :name)
    []))

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

(defn- get-versions
  [{:keys [db version-table]}]
  (try
    (jdbc/query
      (db-spec db)
      [(format "SELECT version FROM %s
               ORDER BY version ASC" version-table)]
      :row-fn :version
      )))

(defn- update-schema-version
  "SQL recording the outcome of a migration in the version table: an insert of
  the version just applied, or -- when `from` is given, meaning that version was
  rolled back down to `version` -- a delete of the rolled-back version's row."
  [version-table from version]
  (if from
    (delete-schema-version version-table from)
    (format "INSERT INTO %s (version,created) VALUES (%s,CURRENT_TIMESTAMP)"
            version-table version)))

(defn- execute-statement!
  "Execute a single SQL statement on the supplied connection.

  Deliberately avoids `jdbc/db-do-commands`, which submits the statement with
  `Statement.addBatch`/`executeBatch`.  The SQL Server driver reads only the
  first result of a batch response, so for a batch holding more than one T-SQL
  statement -- which includes anything of the form `IF <cond> <statement>` --
  two things go wrong:

    * the ENVCHANGE token announcing the transaction that SET IMPLICIT_TRANSACTIONS
      (how the driver implements autocommit=false) opened is never read, so the
      COMMIT that follows carries a stale transaction descriptor and the server
      rejects it with `New request is not allowed to start because it should come
      with valid transaction descriptor` (Msg 3989); and

    * errors raised by any statement after the first are discarded, so a failed
      migration reports success while doing nothing.

  `Statement.execute` has neither problem.  Autocommit is left on: SQL Server
  autocommits DDL anyway, and the per-statement transaction `db-do-commands`
  opened is exactly what provoked Msg 3989."
  [^Connection conn ^String sql]
  (with-open [^Statement stmt (.createStatement conn)]
    (.execute stmt sql)))

(defn- run-migration!
  [{:keys [db version-table]} from version commands]
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
                                       (sql-statements sql {:version version
                                                            :command command})))
                                   commands))]
        ;; One connection for the whole migration.  Session-scoped state --
        ;; `SET IDENTITY_INSERT` most notably -- has to survive from one
        ;; statement to the next, which it cannot do when each statement logs
        ;; in on a connection of its own.
        (with-open [^Connection conn (jdbc/get-connection (db-spec db))]
          (doseq [cmd sql-commands]
            (log/info "Executing: " (pr-str  cmd))
            (execute-statement! conn cmd))
          (if from
            (log/info "Rolled back version" from "- database is now at version" version)
            (log/info "Updating version to: " version))
          (execute-statement! conn (update-schema-version version-table from version)))))))

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
  (versions [this]
    (get-versions config))
  (migrate [this version commands]
    (run-migration! config nil version (validate-commands commands)))
  (migrate [this from version commands]
    (run-migration! config from version (validate-commands commands))))
