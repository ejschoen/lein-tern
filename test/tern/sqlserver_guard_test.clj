(ns tern.sqlserver-guard-test
  "The SQL Server index-existence guard without a server.

  The guard's query is checked by replacing `clojure.java.jdbc/query` with a
  stub that captures the parameter vector it is handed.  The two index
  generators are checked with the guard itself replaced and `*db*` bound, so
  the guard is consulted (with `*db*` nil, as in `tern.sqlserver-test`, the
  generators never ask it).  `:drop-columns` is checked the same way: its
  dependent-index and default-constraint lookups are replaced by a fake catalog
  that answers only for the bound `*db*` and the exact table and column."
  (:require [tern.sqlserver    :as sqlserver]
            [clojure.java.jdbc :as jdbc]
            [expectations      :refer :all]))

(def ^:private db
  {:subprotocol "sqlserver" :host "sqlserver.invalid" :port "1433" :database "i2kdocs"})

(defn- guard-query-params
  "Asks the guard about `index` on `table`, with jdbc/query stubbed to capture
  its parameter vector and return a canned row; returns the captured vector."
  [table index]
  (let [captured (atom nil)]
    (with-redefs [jdbc/query (fn [_ sql-params & _]
                               (reset! captured sql-params)
                               {:1 1})]
      (#'sqlserver/index-exists? db table index))
    @captured))

;; The guard names the database with an empty schema part: "i2kdocs..table".
(expect ["SELECT 1 where indexproperty(object_id(?),?,'IndexID') is not null"
         "i2kdocs..pending_updates"
         "pending_updates_lease_idx"]
        (guard-query-params "pending_updates" "pending_updates_lease_idx"))

;; A reserved-word table arrives bracketed from to-sql-name and keeps its brackets.
(expect ["SELECT 1 where indexproperty(object_id(?),?,'IndexID') is not null"
         "i2kdocs..[user]"
         "user_name_idx"]
        (guard-query-params (sqlserver/to-sql-name :user) "user_name_idx"))

(defn- generate-with-guard
  "generate-sql for `command` with the guard answering `answer`, *db* bound to
  a non-nil map and an empty plan."
  [answer command]
  ;; The stub says yes only for the exact (db, table, index) a generator must pass, so swapped or wrong arguments at a call site fail.
  (with-redefs [sqlserver/index-exists? (fn [d t i] (and (= [d t i] [db "t" "x"]) answer))]
    (binding [sqlserver/*db*   db
              sqlserver/*plan* (atom [])]
      (sqlserver/generate-sql command))))

;; DROP INDEX is emitted only when the guard finds the index.
(expect ["DROP INDEX x ON t"]
        (generate-with-guard true {:drop-index :x :on :t}))

(expect nil
        (generate-with-guard false {:drop-index :x :on :t}))

;; CREATE INDEX is skipped when the guard finds the index, emitted when it does not.
(expect nil
        (generate-with-guard true {:create-index :x :on :t :columns [:a]}))

(expect ["CREATE  INDEX x ON t (a)"]
        (generate-with-guard false {:create-index :x :on :t :columns [:a]}))

;; ---------------------------------------------------------------------------
;; :drop-columns drops what depends on the column first: SQL Server refuses to
;; drop a column that an index or a default constraint depends on (error 5074).

(defn- dropping
  "An :alter-table command dropping `columns` from `table`."
  [table & columns]
  {:alter-table table :drop-columns (vec columns)})

(defn- migrate-with-dependents
  "Runs `commands` through generate-sql as run-migration! does: *db* bound, one
  plan atom starting at `plan`, each command appended to the plan after its SQL
  is generated.  Every column exists and has no check constraint; `indexes` and
  `defaults` map [table column] to the names the two dependent lookups find, and
  the guard finds exactly those indexes.  The lookups answer only when handed the
  bound *db*, so a call site that passes nil or another db finds nothing.
  Returns the statements and the plan entries generate-sql itself added."
  [plan indexes defaults commands]
  (let [catalog (set (for [[[t _] names] indexes, i names] [t i]))]
    (with-redefs [sqlserver/column-exists?          (fn [_ _ _] true)
                  sqlserver/get-column-constraints  (fn [_ _ _] [])
                  sqlserver/get-column-indexes      (fn [d t c] (if (= d db) (get indexes [t c] []) []))
                  sqlserver/get-column-defaults     (fn [d t c] (if (= d db) (get defaults [t c] []) []))
                  sqlserver/index-exists?           (fn [d t i] (and (= d db) (contains? catalog [t i])))]
      (binding [sqlserver/*db*   db
                sqlserver/*plan* (atom plan)]
        (reduce (fn [acc command]
                  (let [before (count @sqlserver/*plan*)
                        sql    (doall (sqlserver/generate-sql command))
                        added  (doall (drop before @sqlserver/*plan*))]
                    (swap! sqlserver/*plan* concat [command])
                    (-> acc (update :sql into sql) (update :added into added))))
                {:sql [] :added []}
                commands)))))

(def ^:private xom-indexes  {["pending_updates" "lease_expires_at"] ["pending_updates_lease_idx"]})
(def ^:private xom-defaults {["pending_updates" "lease_owner"] ["DF_pending_updates_lease_owner"]})

;; XOM's failure without its :drop-index: the index and the default go first, then the columns.
(expect ["drop index if exists pending_updates_lease_idx on pending_updates"
         "ALTER TABLE pending_updates DROP constraint if exists DF_pending_updates_lease_owner"
         "ALTER TABLE pending_updates DROP column lease_owner,lease_expires_at"]
        (:sql (migrate-with-dependents [] xom-indexes xom-defaults [(dropping :pending_updates :lease_owner :lease_expires_at)])))

;; With nothing depending on the columns, only the column drop.
(expect ["ALTER TABLE pending_updates DROP column lease_owner,lease_expires_at"]
        (:sql (migrate-with-dependents [] {} {} [(dropping :pending_updates :lease_owner :lease_expires_at)])))

(def ^:private expected-indexes-sql
  "SELECT i.name FROM sys.indexes i JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id JOIN sys.objects o ON o.object_id = i.object_id WHERE o.name = ? AND c.name = ? AND i.name IS NOT NULL AND i.is_primary_key = 0 AND i.is_unique_constraint = 0")

(def ^:private expected-defaults-sql
  "SELECT d.name FROM sys.default_constraints d JOIN sys.columns c ON c.object_id = d.parent_object_id AND c.column_id = d.parent_column_id JOIN sys.objects o ON o.object_id = d.parent_object_id WHERE o.name = ? AND c.name = ?")

(defn- dependent-lookup-queries
  "What the two dependent lookups send when generate-sql drops `column` from
  `table`: [database sql-params] for each jdbc/query call, captured by a stub
  (the column exists and has no check constraint)."
  [table column]
  (let [captured (atom [])]
    (with-redefs [sqlserver/column-exists?         (fn [_ _ _] true)
                  sqlserver/get-column-constraints (fn [_ _ _] [])
                  jdbc/query                       (fn [spec sql-params & _]
                                                     (swap! captured conj [(:database spec) sql-params])
                                                     [])]
      (binding [sqlserver/*db*   db
                sqlserver/*plan* (atom [])]
        (doall (sqlserver/generate-sql (dropping table column)))))
    @captured))

;; The lookups read sys.indexes and sys.default_constraints (information_schema lists
;; neither) in the bound database, with the table and column as catalog names:
;; unbracketed and snake_case, as column-exists? is handed them.
(expect [["i2kdocs" [expected-indexes-sql "user" "lease_owner"]]
         ["i2kdocs" [expected-defaults-sql "user" "lease_owner"]]]
        (dependent-lookup-queries :user :lease-owner))

(def ^:private two-indexes
  {["pending_updates" "lease_expires_at"] ["pending_updates_lease_idx" "pending_updates_other_idx"]})

;; An index a :drop-index earlier in the migration drops is neither dropped again nor
;; added to the plan again; the column's other index still is.
(expect {:sql   ["drop index if exists pending_updates_other_idx on pending_updates"
                 "ALTER TABLE pending_updates DROP column lease_expires_at"]
         :added [{:drop-index :pending_updates_other_idx :on :pending_updates}]}
        (migrate-with-dependents [{:drop-index :pending_updates_lease_idx :on :pending_updates}]
                                 two-indexes {} [(dropping :pending_updates :lease_expires_at)]))

;; Likewise an index the same :alter-table drops with its own :drop-index key.
(expect {:sql   ["drop index if exists pending_updates_lease_idx on pending_updates"
                 "drop index if exists pending_updates_other_idx on pending_updates"
                 "ALTER TABLE pending_updates DROP column lease_expires_at"]
         :added [{:drop-index :pending_updates_other_idx :on :pending_updates}]}
        (migrate-with-dependents [] two-indexes {}
                                 [(assoc (dropping :pending_updates :lease_expires_at)
                                         :drop-index [:pending_updates_lease_idx])]))

;; A dependent index the generator drops is in the plan, so a :create-index of the same
;; name later in the migration is emitted, not skipped because the catalog (read before
;; anything runs) still lists it.
(expect ["drop index if exists pending_updates_lease_idx on pending_updates"
         "ALTER TABLE pending_updates DROP column lease_expires_at"
         "CREATE  INDEX pending_updates_lease_idx ON pending_updates (state)"]
        (:sql (migrate-with-dependents [] xom-indexes {}
                                       [(dropping :pending_updates :lease_expires_at)
                                        {:create-index :pending_updates_lease_idx :on :pending_updates :columns [:state]}])))

(def ^:private shared-index
  {["pending_updates" "lease_owner"]      ["pending_updates_lease_idx"]
   ["pending_updates" "lease_expires_at"] ["pending_updates_lease_idx"]
   ["pending_updates" "leased_at"]        ["pending_updates_lease_idx"]})

;; An index on several dropped columns is dropped, and added to the plan, once: within one
;; command and across two.
(expect {:sql   ["drop index if exists pending_updates_lease_idx on pending_updates"
                 "ALTER TABLE pending_updates DROP column lease_owner,lease_expires_at"
                 "ALTER TABLE pending_updates DROP column leased_at"]
         :added [{:drop-index :pending_updates_lease_idx :on :pending_updates}]}
        (migrate-with-dependents [] shared-index {}
                                 [(dropping :pending_updates :lease_owner :lease_expires_at)
                                  (dropping :pending_updates :leased_at)]))
