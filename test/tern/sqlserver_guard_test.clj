(ns tern.sqlserver-guard-test
  "The SQL Server index-existence guard without a server.

  The guard's query is checked by replacing `clojure.java.jdbc/query` with a
  stub that captures the parameter vector it is handed.  The two index
  generators are checked with the guard itself replaced and `*db*` bound, so
  the guard is consulted (with `*db*` nil, as in `tern.sqlserver-test`, the
  generators never ask it).  `:drop-columns` is checked the same way: its
  dependent-index and default-constraint lookups are replaced by a fake catalog
  that answers only for the bound `*db*` and the exact table and column.  The
  create guard's reading of the plan is checked with the guard saying yes for the
  exact (db, table, index), the add-constraints guard's with the foreign-key
  lookup saying yes for the exact (db, constraint), the create-table guard's with
  the table lookup saying yes for the exact (db, table), and the check-constraint
  lookup by capturing the query it sends."
  (:require [tern.sqlserver    :as sqlserver]
            [tern.db           :as tern-db]
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
;; While the guard finds the index, CREATE INDEX is still emitted when an earlier command
;; in the migration drops it: a {:drop-index i :on t} command, an :alter-table's
;; :drop-index key or a :drop-table of the table, the table and index compared as
;; catalog names.

(defn- create-after
  "generate-sql for a :create-index of `index` on `table` (:t unless given), *db*
  bound, the guard saying yes only for the bound db and `guard`, the [table index]
  strings the generator must hand it: to-sql-name's, so a reserved-word table
  arrives bracketed (\"[user]\") and a kebab-case name in snake_case; by default
  \"t\" and the name of `index`, a snake_case keyword.  Returns [what an empty plan
  gives, what `plan` gives]: the first is nil only if the guard said yes, so the
  second emitting the CREATE is the plan's doing."
  ([plan index] (create-after plan :t index ["t" (name index)]))
  ([plan table index guard]
   (with-redefs [sqlserver/index-exists? (fn [d t i] (= [d t i] (into [db] guard)))]
     (vec (for [p [[] plan]]
            (binding [sqlserver/*db*   db
                      sqlserver/*plan* (atom p)]
              (sqlserver/generate-sql {:create-index index :on table :columns [:a]})))))))

;; An :alter-table's :drop-index key (a vector, no :on) counts as a drop of each index on
;; that table.
(expect [nil ["CREATE  INDEX x ON t (a)"]]
        (create-after [{:alter-table :t :drop-index [:x]}] :x))

;; A kebab-case drop and a snake_case create name the same catalog index.
(expect [nil ["CREATE  INDEX my_idx ON t (a)"]]
        (create-after [{:drop-index :my-idx :on :t}] :my_idx))

;; So do a drop that names the index and the table as strings and a keyword create.
(expect [nil ["CREATE  INDEX my_idx ON t (a)"]]
        (create-after [{:drop-index "my_idx" :on "t"}] :my_idx))

;; The table matters: a drop of x on another table, in either form, leaves the create on t
;; skipped, and so does a :drop-index that names no table.
(expect [[nil nil] [nil nil] [nil nil]]
        [(create-after [{:drop-index :x :on :u}] :x)
         (create-after [{:alter-table :u :drop-index [:x]}] :x)
         (create-after [{:drop-index :x}] :x)])

;; Every index in an :alter-table's :drop-index vector counts, not only the first.
(expect [nil ["CREATE  INDEX x ON t (a)"]]
        (create-after [{:alter-table :t :drop-index [:y :x]}] :x))

;; So does every index in a set, which the :alter-table generator accepts too.
(expect [nil ["CREATE  INDEX x ON t (a)"]]
        (create-after [{:alter-table :t :drop-index #{:x}}] :x))

;; A kebab-case table, in either drop form, is the catalog table the snake_case create
;; names.
(expect [[nil ["CREATE  INDEX x ON my_table (a)"]] [nil ["CREATE  INDEX x ON my_table (a)"]]]
        [(create-after [{:drop-index :x :on :my-table}] :my_table :x ["my_table" "x"])
         (create-after [{:alter-table :my-table :drop-index [:x]}] :my_table :x ["my_table" "x"])])

;; A kebab-case create is the catalog index a snake_case drop names.
(expect [nil ["CREATE  INDEX my_idx ON t (a)"]]
        (create-after [{:drop-index :my_idx :on :t}] :t :my-idx ["t" "my_idx"]))

;; A reserved-word table reaches the guard bracketed, as OBJECT_ID needs it, while the
;; plan's drops of it, in either form, are compared as the catalog name user.
(expect [[nil ["CREATE  INDEX x ON [user] (a)"]] [nil ["CREATE  INDEX x ON [user] (a)"]]]
        [(create-after [{:drop-index :x :on :user}] :user :x ["[user]" "x"])
         (create-after [{:alter-table :user :drop-index [:x]}] :user :x ["[user]" "x"])])

;; Dropping a table drops its indexes: after a :drop-table of the table (kebab or snake
;; case) the create is emitted, though the catalog, read before anything runs, still
;; shows the old table's index ...
(expect [[nil ["CREATE  INDEX x ON t (a)"]] [nil ["CREATE  INDEX x ON my_table (a)"]]]
        [(create-after [{:drop-table :t}] :x)
         (create-after [{:drop-table :my-table}] :my_table :x ["my_table" "x"])])

;; ... and a :drop-table of another table leaves it skipped.
(expect [nil nil]
        (create-after [{:drop-table :u}] :x))

(defn- add-constraint-after
  "generate-sql for an :alter-table of `table` adding the foreign keys `fks` (fk_x
  unless given), each on (a) referencing u(id), *db* bound, the lookup saying yes
  only for the bound db and the names in `existing`, as the generator must hand
  them over (to-sql-name's; by default \"fk_x\").  Returns [what an empty plan
  gives, what `plan` gives]: the first is empty only if the lookup said yes for
  each of `fks`."
  ([plan table] (add-constraint-after plan table [:fk_x] #{"fk_x"}))
  ([plan table fks existing]
   (with-redefs [sqlserver/foreign-key-exists? (fn [d c] (and (= d db) (contains? existing c)))]
     (vec (for [p [[] plan]]
            (binding [sqlserver/*db*   db
                      sqlserver/*plan* (atom p)]
              (vec (sqlserver/generate-sql {:alter-table     table
                                            :add-constraints (vec (for [fk fks] [fk "(a) REFERENCES u(id)"]))}))))))))

;; :add-constraints follows the same rule: a foreign key the catalog shows is added again
;; after an earlier :drop-constraints of it on the table (the table compared as a catalog
;; name, so :my-t is :my_t) or a :drop-table of the table, and not after either on
;; another table.
(expect [[[] ["ALTER TABLE my_t ADD CONSTRAINT fk_x FOREIGN KEY (a) REFERENCES u(id)"]]
         [[] ["ALTER TABLE t ADD CONSTRAINT fk_x FOREIGN KEY (a) REFERENCES u(id)"]]
         [[] []]
         [[] []]]
        [(add-constraint-after [{:alter-table :my-t :drop-constraints [:fk_x]}] :my_t)
         (add-constraint-after [{:drop-table :t}] :t)
         (add-constraint-after [{:alter-table :u :drop-constraints [:fk_x]}] :t)
         (add-constraint-after [{:drop-table :u}] :t)])

;; Every constraint in a :drop-constraints vector counts, not only the first: both foreign
;; keys are added again.
(expect [[] ["ALTER TABLE t ADD CONSTRAINT fk_a FOREIGN KEY (a) REFERENCES u(id),CONSTRAINT fk_b FOREIGN KEY (a) REFERENCES u(id)"]]
        (add-constraint-after [{:alter-table :t :drop-constraints [:fk_a :fk_b]}] :t [:fk_a :fk_b] #{"fk_a" "fk_b"}))

;; A kebab-case constraint name is the catalog name of its snake_case spelling, on either
;; side: dropped as :fk-x and added as :fk_x, or dropped as :fk_x and added as :fk-x.
(expect [[[] ["ALTER TABLE t ADD CONSTRAINT fk_x FOREIGN KEY (a) REFERENCES u(id)"]]
         [[] ["ALTER TABLE t ADD CONSTRAINT fk_x FOREIGN KEY (a) REFERENCES u(id)"]]]
        [(add-constraint-after [{:alter-table :t :drop-constraints [:fk-x]}] :t)
         (add-constraint-after [{:alter-table :t :drop-constraints [:fk_x]}] :t [:fk-x] #{"fk_x"})])

;; A reserved-word table: the plan's drop on :user is compared as the catalog name user;
;; the lookup is handed only the constraint, and the statement brackets the table.
(expect [[] ["ALTER TABLE [user] ADD CONSTRAINT fk_x FOREIGN KEY (a) REFERENCES u(id)"]]
        (add-constraint-after [{:alter-table :user :drop-constraints [:fk_x]}] :user))

(defn- create-table-after
  "generate-sql for a :create-table of :t (column a INT), *db* bound, the lookup
  saying the table exists only for the bound db and \"t\".  Returns [what an empty
  plan gives, what `plan` gives]: the first is nil only if the lookup said yes."
  [plan]
  (with-redefs [sqlserver/table-exists? (fn [d t] (= [d t] [db "t"]))]
    (vec (for [p [[] plan]]
           (binding [sqlserver/*db*   db
                     sqlserver/*plan* (atom p)]
             (sqlserver/generate-sql {:create-table :t :columns [[:a "INT"]]}))))))

;; :create-table reads the plan's drops the same way: after a :drop-table that names the
;; table as a string, or carries a :sqlserver override, CREATE TABLE is emitted though the
;; catalog still shows the table.
(expect [[nil ["CREATE TABLE t (a INT)"]] [nil ["CREATE TABLE t (a INT)"]]]
        [(create-table-after [{:drop-table "t"}])
         (create-table-after [{:drop-table :t :sqlserver "DROP TABLE t"}])])

(def ^:private chathistory-up
  "i2kdb 20250427121101-chathistory_conversations.edn's :up: the table is dropped and
  created again with two foreign keys and five indexes; 20240622220001 had created
  the indexes chathistory_clients_id, chathistory_users_id and chathistory_created
  under the same names."
  [{:drop-table :chathistory}
   {:create-table :chathistory
    :columns [[:clients_id "INT" "NOT NULL"]
              [:users_id "INT" "NOT NULL"]
              [:conversation_id "VARCHAR(36)" "NOT NULL"]
              [:metadata_json "LONGTEXT" "COLLATE utf8_general_ci" "NOT NULL"]
              [:messages_json "LONGTEXT" "COLLATE utf8_general_ci" "NOT NULL"]
              [:created "TIMESTAMP" "NOT NULL" "DEFAULT CURRENT_TIMESTAMP"]
              [:updated "TIMESTAMP" "NOT NULL" "DEFAULT CURRENT_TIMESTAMP"]]}
   {:alter-table :chathistory
    :add-constraints [[:fk_chathistory_clients "(clients_id) REFERENCES clients(id) ON DELETE CASCADE"]
                      [:fk_chathistory_users "(users_id) REFERENCES users(id) ON DELETE CASCADE"]]}
   {:create-index :chathistory_clients_id :on :chathistory :columns [:clients_id]}
   {:create-index :chathistory_users_id :on :chathistory :columns [:users_id]}
   {:create-index :chathistory_conversation_id :on :chathistory :columns [:clients_id :users_id :conversation_id]}
   {:create-index :chathistory_created :on :chathistory :columns [:created]}
   {:create-index :chathistory_updated :on :chathistory :columns [:updated]}])

(defn- migrate-over-catalog
  "Runs `commands` as run-migration! does: *db* bound, one plan, each command's
  statements its :sqlserver override or what generate-sql gives (flattened by
  tern.db/sql-statements), the command appended to the plan after them.  The
  catalog, read before anything runs, answers with `tables`, `columns`, `indexes`
  and `fks`, each a set (or predicate) of what the generator hands the lookup: a
  table, [table column], [table index], a constraint; each lookup answers only for
  the bound *db*.  Returns the statements."
  [{:keys [tables columns indexes fks] :or {tables #{} columns #{} indexes #{} fks #{}}} commands]
  (with-redefs [sqlserver/table-exists?       (fn [d t] (boolean (and (= d db) (tables t))))
                sqlserver/column-exists?      (fn [d t c] (boolean (and (= d db) (columns [t c]))))
                sqlserver/index-exists?       (fn [d t i] (boolean (and (= d db) (indexes [t i]))))
                sqlserver/foreign-key-exists? (fn [d c] (boolean (and (= d db) (fks c))))]
    (binding [sqlserver/*db*   db
              sqlserver/*plan* (atom [])]
      (reduce (fn [acc command]
                (let [sql (doall (tern-db/sql-statements (or (:sqlserver command) (sqlserver/generate-sql command))))]
                  (swap! sqlserver/*plan* concat [command])
                  (into acc sql)))
              []
              commands))))

(defn- migrate-over-old-table
  "migrate-over-catalog against a catalog in which `table` exists with every index
  and foreign key asked about."
  [table commands]
  (migrate-over-catalog {:tables #{table} :indexes (fn [[t _]] (= t table)) :fks (constantly true)} commands))

;; The real shape: chathistory dropped and created again gets back both foreign keys and
;; all five indexes, though the catalog the SQL is generated against still shows them all.
(expect ["DROP TABLE chathistory"
         "CREATE TABLE chathistory (clients_id INT NOT NULL, users_id INT NOT NULL, conversation_id VARCHAR(36) NOT NULL, metadata_json varchar(max)  NOT NULL, messages_json varchar(max)  NOT NULL, created datetime NOT NULL DEFAULT CURRENT_TIMESTAMP, updated datetime NOT NULL DEFAULT CURRENT_TIMESTAMP)"
         "ALTER TABLE chathistory ADD CONSTRAINT fk_chathistory_clients FOREIGN KEY (clients_id) REFERENCES clients(id) ON DELETE CASCADE,CONSTRAINT fk_chathistory_users FOREIGN KEY (users_id) REFERENCES users(id) ON DELETE CASCADE"
         "CREATE  INDEX chathistory_clients_id ON chathistory (clients_id)"
         "CREATE  INDEX chathistory_users_id ON chathistory (users_id)"
         "CREATE  INDEX chathistory_conversation_id ON chathistory (clients_id, users_id, conversation_id)"
         "CREATE  INDEX chathistory_created ON chathistory (created)"
         "CREATE  INDEX chathistory_updated ON chathistory (updated)"]
        (migrate-over-old-table "chathistory" chathistory-up))

(def ^:private feedback-up
  "i2kdb 20190509100957-denormalize-feedback-client-and-user.edn's :up: one
  :drop-constraints drops both of feedback's foreign keys, which a later
  :add-constraints adds again."
  [{:alter-table :feedback
    :add-columns [[:client "VARCHAR(255)" "NULL"]
                  [:user "VARCHAR(255)" "NULL"]]
    :modify-columns [[:users_id "INT(11)" "NULL"]]
    :drop-constraints [:fk_feedback_clients_id :fk_feedback_users_id]}
   {:update "update feedback,clients set feedback.client=clients.name where feedback.clients_id=clients.id"
    :h2 "update feedback set client=(select name from clients where feedback.clients_id=clients.id)"
    :sqlserver "update feedback set client=(select name from clients where feedback.clients_id=clients.id)"}
   {:update "update feedback,users set feedback.user=users.name where feedback.users_id=users.id"
    :h2 "update feedback set `user`=(select name from users where feedback.users_id=users.id)"
    :sqlserver "update feedback set [user]=(select name from users where feedback.users_id=users.id)"}
   {:alter-table :feedback
    :add-constraints [[:fk_feedback_clients_id "(clients_id) REFERENCES clients(id) ON DELETE SET NULL"]
                      [:fk_feedback_users_id "(users_id) REFERENCES users(id) ON DELETE SET NULL"]]
    :modify-columns [[:client "VARCHAR(255)" "NOT NULL"]
                     [:user "VARCHAR(255)" "NOT NULL"]]}
   {:alter-table :users
    :drop-constraints [:fk_users_clients_id]}
   {:alter-table :users
    :add-constraints [[:fk_users_clients_id "(clients_id) REFERENCES clients(id) ON DELETE CASCADE"]]
    :sqlserver ["alter table users add constraint fk_users_clients_id foreign key (clients_id) references clients(id)"
                "create or alter trigger delete_client on clients instead of delete as begin set nocount on; delete from users where clients_id in (select id from deleted); delete from clients where id in (select id from deleted); end"]}
   {:create-index :feedback_client_idx :on :feedback :columns [:client]}])

;; The real shape: both of feedback's foreign keys are added again, in one statement, though
;; the catalog the SQL is generated against still shows them; the overrides run as written.
(expect ["ALTER TABLE feedback ALTER COLUMN users_id int NULL"
         "ALTER TABLE feedback DROP constraint if exists fk_feedback_clients_id,fk_feedback_users_id"
         "ALTER TABLE feedback ADD client VARCHAR(255) NULL,[user] VARCHAR(255) NULL"
         "update feedback set client=(select name from clients where feedback.clients_id=clients.id)"
         "update feedback set [user]=(select name from users where feedback.users_id=users.id)"
         "ALTER TABLE feedback ALTER COLUMN client VARCHAR(255) NOT NULL"
         "ALTER TABLE feedback ALTER COLUMN [user] VARCHAR(255) NOT NULL"
         "ALTER TABLE feedback ADD CONSTRAINT fk_feedback_clients_id FOREIGN KEY (clients_id) REFERENCES clients(id) ON DELETE SET NULL,CONSTRAINT fk_feedback_users_id FOREIGN KEY (users_id) REFERENCES users(id) ON DELETE SET NULL"
         "ALTER TABLE users DROP constraint if exists fk_users_clients_id"
         "alter table users add constraint fk_users_clients_id foreign key (clients_id) references clients(id)"
         "create or alter trigger delete_client on clients instead of delete as begin set nocount on; delete from users where clients_id in (select id from deleted); delete from clients where id in (select id from deleted); end"
         "CREATE  INDEX feedback_client_idx ON feedback (client)"]
        (migrate-over-catalog {:tables  #{"feedback" "users" "clients"}
                               :columns #{["feedback" "clients_id"] ["feedback" "users_id"]}
                               :fks     #{"fk_feedback_clients_id" "fk_feedback_users_id" "fk_users_clients_id"}}
                              feedback-up))

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

(def ^:private expected-constraints-sql
  "select constraint_name from information_schema.constraint_column_usage where table_catalog=? and table_name=? and column_name=?")

(def ^:private check-constraints
  {["user" "name"]           ["CK_user_name"]
   ["concept_tags" "public"] ["CK_concept_tags_public"]})

(defn- dropping-with-check
  "Drops `column` from `table` with jdbc/query stubbed as the catalog: it captures
  [database sql-params] for every query and answers the check-constraint lookup
  (applying the :row-fn it is handed, as java.jdbc does) only when handed the bound
  database and a [table column] of `check-constraints` as catalog names; every
  other query finds nothing.  The column exists only for the bound db and those
  exact names.  Returns {:queries [...] :sql [...]}."
  [table column]
  (let [captured (atom [])]
    (with-redefs [sqlserver/column-exists? (fn [d t c] (and (= d db) (contains? check-constraints [t c])))
                  jdbc/query               (fn [spec [sql & params :as sql-params] & {:keys [row-fn result-set-fn]
                                                                                       :or   {row-fn identity result-set-fn doall}}]
                                             (swap! captured conj [(:database spec) sql-params])
                                             (let [[catalog t c] params
                                                   rows (if (and (= (:database spec) "i2kdocs")
                                                                 (= sql expected-constraints-sql)
                                                                 (= catalog "i2kdocs"))
                                                          (map (fn [n] {:constraint_name n}) (get check-constraints [t c]))
                                                          [])]
                                               (result-set-fn (map row-fn rows))))]
      (binding [sqlserver/*db*   db
                sqlserver/*plan* (atom [])]
        (let [sql (vec (sqlserver/generate-sql (dropping table column)))]
          {:queries @captured :sql sql})))))

;; The check-constraint lookup (information_schema.constraint_column_usage, compared with
;; table_name = ? and column_name = ?) is handed catalog names, as the dependent lookups
;; are: a reserved-word table (user) or column (public) unbracketed, a kebab-case table in
;; snake_case.
(expect [[["i2kdocs" [expected-constraints-sql "i2kdocs" "user" "name"]]
          ["i2kdocs" [expected-indexes-sql "user" "name"]]
          ["i2kdocs" [expected-defaults-sql "user" "name"]]]
         [["i2kdocs" [expected-constraints-sql "i2kdocs" "concept_tags" "public"]]
          ["i2kdocs" [expected-indexes-sql "concept_tags" "public"]]
          ["i2kdocs" [expected-defaults-sql "concept_tags" "public"]]]]
        [(:queries (dropping-with-check :user :name))
         (:queries (dropping-with-check :concept-tags :public))])

;; So a check constraint it finds is dropped before the column, in a statement that names
;; the table and the column as to-sql-name gives them.
(expect [["ALTER TABLE [user] DROP constraint if exists CK_user_name"
          "ALTER TABLE [user] DROP column name"]
         ["ALTER TABLE concept_tags DROP constraint if exists CK_concept_tags_public"
          "ALTER TABLE concept_tags DROP column [public]"]]
        [(:sql (dropping-with-check :user :name))
         (:sql (dropping-with-check :concept-tags :public))])

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
