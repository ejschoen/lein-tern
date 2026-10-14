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

;; XOM's failure without its :drop-index: the index goes first, dropped only if, when the
;; statement runs, it still contains one of the columns the command drops (the exact
;; predicate text for this table, index and the two columns), then the default and the
;; columns together, in one ALTER TABLE that SQL Server applies whole or not at all.
(expect ["IF EXISTS (SELECT 1 FROM sys.indexes i JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id JOIN sys.objects o ON o.object_id = i.object_id WHERE o.name = 'pending_updates' AND i.name = 'pending_updates_lease_idx' AND c.name IN ('lease_owner', 'lease_expires_at')) DROP INDEX [pending_updates_lease_idx] ON pending_updates"
         "ALTER TABLE pending_updates DROP constraint if exists DF_pending_updates_lease_owner, column lease_owner,lease_expires_at"]
        (:sql (migrate-with-dependents [] xom-indexes xom-defaults [(dropping :pending_updates :lease_owner :lease_expires_at)])))

;; With nothing depending on the columns, only the column drop.
(expect ["ALTER TABLE pending_updates DROP column lease_owner,lease_expires_at"]
        (:sql (migrate-with-dependents [] {} {} [(dropping :pending_updates :lease_owner :lease_expires_at)])))

(defn- if-holds
  "The statement expected for `index` on `table` when the command drops `columns`
  (catalog names, quote-free; `on` is the table as the DROP names it, `table`
  unless given)."
  ([table index columns] (if-holds table index columns table))
  ([table index columns on]
   (str "IF EXISTS (SELECT 1 FROM sys.indexes i JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id"
        " JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id JOIN sys.objects o ON o.object_id = i.object_id"
        " WHERE o.name = '" table "' AND i.name = '" index "' AND c.name IN ("
        (clojure.string/join ", " (map #(str "'" % "'") columns)) "))"
        " DROP INDEX [" index "] ON " on)))

(def ^:private stmt-prefix
  "IF EXISTS (SELECT 1 FROM sys.indexes i JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id JOIN sys.objects o ON o.object_id = i.object_id WHERE o.name = 'pending_updates' AND ")

;; Names that need quoting: in the predicate a T-SQL literal doubles a single quote; in the
;; DROP the index is always bracketed, a ] doubled inside (as QUOTENAME does; a [ needs no
;; escape), so a quote, a bracket or a space in the name is valid T-SQL.
(expect [(str stmt-prefix "i.name = 'ix''q' AND c.name IN ('state', 'o''k')) DROP INDEX [ix'q] ON pending_updates")
         (str stmt-prefix "i.name = 'ix]b' AND c.name IN ('state')) DROP INDEX [ix]]b] ON pending_updates")
         (str stmt-prefix "i.name = 'IX sp' AND c.name IN ('state')) DROP INDEX [IX sp] ON pending_updates")]
        [(#'sqlserver/drop-index-if-it-holds :pending_updates "ix'q" ["state" "o'k"])
         (#'sqlserver/drop-index-if-it-holds :pending_updates "ix]b" ["state"])
         (#'sqlserver/drop-index-if-it-holds :pending_updates "IX sp" ["state"])])

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
;; unbracketed and snake_case, as column-exists? is handed them.  `user` and `public`
;; are the reserved words to-sql-name brackets in statements; concept-tags is a real
;; kebab-case table (i2kdb 20160423191540).
(expect [[["i2kdocs" [expected-indexes-sql "user" "lease_owner"]]
          ["i2kdocs" [expected-defaults-sql "user" "lease_owner"]]]
         [["i2kdocs" [expected-indexes-sql "concept_tags" "public"]]
          ["i2kdocs" [expected-defaults-sql "concept_tags" "public"]]]]
        [(dependent-lookup-queries :user :lease-owner)
         (dependent-lookup-queries :concept-tags :public)])

(defn- lookups-on-three-rows
  "What the two lookups themselves return when the catalog answers three rows:
  jdbc/query is stubbed to apply the :row-fn and :result-set-fn it is handed, as
  java.jdbc does (defaults: identity and doall), to three rows named i1, i2, i3."
  []
  (with-redefs [jdbc/query (fn [_ _ & {:keys [row-fn result-set-fn]
                                       :or   {row-fn identity result-set-fn doall}}]
                             (result-set-fn (map row-fn [{:name "i1"} {:name "i2"} {:name "i3"}])))]
    [(vec (#'sqlserver/get-column-indexes db "pending_updates" "state"))
     (vec (#'sqlserver/get-column-defaults db "pending_updates" "state"))]))

;; Every row comes back, in order: a column in several indexes (pending_updates.state is
;; in four) needs every one of them dropped.
(expect [["i1" "i2" "i3"] ["i1" "i2" "i3"]]
        (lookups-on-three-rows))

(def ^:private two-indexes
  {["pending_updates" "lease_expires_at"] ["pending_updates_lease_idx" "pending_updates_other_idx"]})

(def ^:private after-planned-drop
  {:sql   [(if-holds "pending_updates" "pending_updates_lease_idx" ["lease_expires_at"])
           (if-holds "pending_updates" "pending_updates_other_idx" ["lease_expires_at"])
           "ALTER TABLE pending_updates DROP column lease_expires_at"]
   :added [{:drop-index :pending_updates_other_idx :on :pending_updates}]})

;; A :drop-index earlier in the migration changes nothing: it emits nothing when its guard
;; misses the index, and whether the index still holds the column is decided when the
;; statement runs.  The plan already holds the earlier entry, so only the other index is
;; added to it.
(expect after-planned-drop
        (migrate-with-dependents [{:drop-index :pending_updates_lease_idx :on :pending_updates}]
                                 two-indexes {} [(dropping :pending_updates :lease_expires_at)]))

(def ^:private with-own-drop-index
  {:sql   ["drop index if exists pending_updates_lease_idx on pending_updates"
           (if-holds "pending_updates" "pending_updates_other_idx" ["lease_expires_at"])
           "ALTER TABLE pending_updates DROP column lease_expires_at"]
   :added [{:drop-index :pending_updates_other_idx :on :pending_updates}]})

;; The command's own :drop-index key is always emitted, so its index is dropped once, by
;; the key, and gets no statement from the dependent list.
(expect with-own-drop-index
        (migrate-with-dependents [] two-indexes {}
                                 [(assoc (dropping :pending_updates :lease_expires_at)
                                         :drop-index [:pending_updates_lease_idx])]))

(def ^:private drop-then-create
  [(if-holds "pending_updates" "pending_updates_lease_idx" ["lease_expires_at"])
   "ALTER TABLE pending_updates DROP column lease_expires_at"
   "CREATE  INDEX pending_updates_lease_idx ON pending_updates (state)"])

;; A dependent index the generator drops is in the plan, so a :create-index of the same
;; name later in the migration is emitted, not skipped because the catalog (read before
;; anything runs) still lists it.
(expect drop-then-create
        (:sql (migrate-with-dependents [] xom-indexes {}
                                       [(dropping :pending_updates :lease_expires_at)
                                        {:create-index :pending_updates_lease_idx :on :pending_updates :columns [:state]}])))

(def ^:private redefining-plan
  [{:drop-index :pending_updates_lease_idx :on :pending_updates}
   {:create-index :pending_updates_lease_idx :on :pending_updates :columns [:state]}])

(def ^:private conditional-drop-only
  {:sql   [(if-holds "pending_updates" "pending_updates_lease_idx" ["lease_expires_at"])
           "ALTER TABLE pending_updates DROP column lease_expires_at"]
   :added []})

;; A migration that dropped the index and created it again gets the same conditional
;; statement: at run time the new definition (on state) does not hold the column, so the
;; index is left alone.  The plan already holds the drop, so nothing is added.
(expect conditional-drop-only
        (migrate-with-dependents redefining-plan xom-indexes {} [(dropping :pending_updates :lease_expires_at)]))

(def ^:private conditional-drop-added
  {:sql   [(if-holds "pending_updates" "pending_updates_lease_idx" ["lease_expires_at"])
           "ALTER TABLE pending_updates DROP column lease_expires_at"]
   :added [{:drop-index :pending_updates_lease_idx :on :pending_updates}]})

;; A :create-index alone (its guard skips it while the index exists) gets the same
;; statement, and the index's plan entry is added.
(expect conditional-drop-added
        (migrate-with-dependents [{:create-index :pending_updates_lease_idx :on :pending_updates :columns [:state]}]
                                 xom-indexes {} [(dropping :pending_updates :lease_expires_at)]))

(def ^:private shared-index
  {["pending_updates" "lease_owner"]      ["pending_updates_lease_idx"]
   ["pending_updates" "lease_expires_at"] ["pending_updates_lease_idx"]
   ["pending_updates" "leased_at"]        ["pending_updates_lease_idx"]})

(def ^:private shared-index-twice
  {:sql   [(if-holds "pending_updates" "pending_updates_lease_idx" ["lease_owner" "lease_expires_at"])
           "ALTER TABLE pending_updates DROP column lease_owner,lease_expires_at"
           (if-holds "pending_updates" "pending_updates_lease_idx" ["leased_at"])
           "ALTER TABLE pending_updates DROP column leased_at"]
   :added [{:drop-index :pending_updates_lease_idx :on :pending_updates}]})

;; An index on several dropped columns: one statement per command, its predicate on every
;; column that command drops, and one plan entry for the whole migration.
(expect shared-index-twice
        (migrate-with-dependents [] shared-index {}
                                 [(dropping :pending_updates :lease_owner :lease_expires_at)
                                  (dropping :pending_updates :leased_at)]))

(def ^:private reserved-table
  [(if-holds "user" "user_x_idx" ["name"] "[user]")
   "ALTER TABLE [user] DROP column name"])

;; In the DROP a reserved-word table keeps the brackets to-sql-name gives it; in the
;; predicate it is the catalog name.
(expect reserved-table
        (:sql (migrate-with-dependents [] {["user" "name"] ["user_x_idx"]} {} [(dropping :user :name)])))

(def ^:private recreated-on-second
  [(if-holds "t" "x_idx" ["a" "b"])
   "ALTER TABLE t DROP column a,b"])

;; The predicate lists every column the command drops, not only the one the catalog showed
;; in the index when the SQL was generated: x_idx holds only a then, and if the migration
;; re-creates it on b before this statement runs, it still holds a dropped column and is
;; dropped, so the column drop does not meet 5074.
(expect recreated-on-second
        (:sql (migrate-with-dependents [] {["t" "a"] ["x_idx"]} {} [(dropping :t :a :b)])))
