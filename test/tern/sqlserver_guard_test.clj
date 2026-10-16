(ns tern.sqlserver-guard-test
  "The SQL Server index-existence guard without a server.

  The guard's query is checked by replacing `clojure.java.jdbc/query` with a
  stub that captures the parameter vector it is handed.  The two index
  generators are checked with the guard itself replaced and `*db*` bound, so
  the guard is consulted (with `*db*` nil, as in `tern.sqlserver-test`, the
  generators never ask it).  `:drop-columns` is checked the same way: its
  dependent-index and default-constraint lookups are replaced by a fake catalog
  that answers only for the bound `*db*` and the exact table and column, and its
  refusal of an index that still contains a dropped column is checked by trapping
  tern's exit (`tern.sqlserver/exit!`, replaced by a stub that records the status
  and throws, as System/exit never returns) and capturing `tern.log/error`; two
  cases run tern's own run-migration! with the connection and the executor
  replaced, to show the refusal comes before the first statement, and sets of
  cases have an earlier command's :sqlserver override drop the index, the table
  or a constraint, or an earlier command create an index on the column.  The
  create guard's reading of the plan is checked with the guard saying yes for the
  exact (db, table, index), the add-constraints guard's with the foreign-key
  lookup saying yes for the exact (db, constraint), the create-table guard's with
  the table lookup saying yes for the exact (db, table), and the check-constraint
  lookup by capturing the query it sends."
  (:require [tern.sqlserver    :as sqlserver]
            [tern.db           :as tern-db]
            [tern.log          :as log]
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

;; ... and a :drop-table of another table leaves it skipped.
(expect [nil nil]
        (create-table-after [{:drop-table :u}]))

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
;; :drop-columns: SQL Server refuses to drop a column that an index or a default
;; constraint depends on (error 5074).  The default is dropped in the column's own
;; statement; an index that still contains the column must be dropped earlier in the
;; migration, or tern stops the migration while its SQL is generated.

(defn- dropping
  "An :alter-table command dropping `columns` from `table`."
  [table & columns]
  {:alter-table table :drop-columns (vec columns)})

(defn- fake-catalog
  "Var bindings, for with-redefs-fn, of a catalog in which every column exists and
  has no check constraint, `indexes` and `defaults` map [table column] to the names
  the two dependent lookups find, and the guard finds exactly those indexes.  The
  lookups answer only when handed the bound *db*, so a call site that passes nil or
  another db finds nothing.  tern's exit records its status in `exit` and throws, as
  System/exit never returns; the lines tern logs as errors go to `errors`."
  [indexes defaults exit errors]
  (let [catalog (set (for [[[t _] names] indexes, i names] [t i]))]
    {#'sqlserver/column-exists?         (fn [_ _ _] true)
     #'sqlserver/get-column-constraints (fn [_ _ _] [])
     #'sqlserver/get-column-indexes     (fn [d t c] (if (= d db) (get indexes [t c] []) []))
     #'sqlserver/get-column-defaults    (fn [d t c] (if (= d db) (get defaults [t c] []) []))
     #'sqlserver/index-exists?          (fn [d t i] (and (= d db) (contains? catalog [t i])))
     #'sqlserver/exit!                  (fn [status]
                                          (reset! exit status)
                                          (throw (ex-info "tern requested its exit" {::exit status})))
     #'log/error                        (fn [& args] (swap! errors conj (clojure.string/join " " args)))}))

(defn- exit-thrown?
  [e]
  (contains? (ex-data e) ::exit))

(defn- migrate-with-dependents
  "Runs `commands` as run-migration! does, over fake-catalog: *db* bound, one plan
  atom starting at `plan`, each command's statements its :sqlserver override or
  what generate-sql gives (flattened by tern.db/sql-statements), the command
  appended to the plan after them; a requested exit ends the run there.  Returns
  {:sql the statements generated before any exit, :errors the lines tern logged as
  errors, :exit the status of the requested exit, or nil}."
  [plan indexes defaults commands]
  (let [sql    (atom [])
        errors (atom [])
        exit   (atom nil)]
    (with-redefs-fn (fake-catalog indexes defaults exit errors)
      (fn []
        (binding [sqlserver/*db*   db
                  sqlserver/*plan* (atom plan)]
          (try
            (doseq [command commands]
              (let [statements (doall (tern-db/sql-statements (or (:sqlserver command)
                                                                  (sqlserver/generate-sql command))))]
                (swap! sqlserver/*plan* concat [command])
                (swap! sql into statements)))
            (catch clojure.lang.ExceptionInfo e
              (when-not (exit-thrown? e) (throw e)))))))
    {:sql @sql :errors @errors :exit @exit}))

(defn- refusal
  "The line tern logs when `column` of `table` cannot be dropped because `index` still
  contains it."
  [table column index]
  (str "tern: cannot drop column " table "." column " on SQL Server: index " index " still contains it;"
       " drop the index first in this migration (:drop-index), or remove a stale index by hand"
       " (see the SQL Server migration runbook, section 5a)"))

(def ^:private t-a-index   {["t" "a"] ["ix_a"]})
(def ^:private t-a-default {["t" "a"] ["DF_t_a"]})

(def ^:private column-and-default
  "ALTER TABLE t DROP constraint if exists DF_t_a, column a")

;; (a) An index on the dropped column that the migration does not drop: tern logs one line
;; naming the table, the column and the index, and requests its exit; nothing is generated.
(expect {:sql    []
         :errors ["tern: cannot drop column t.a on SQL Server: index ix_a still contains it; drop the index first in this migration (:drop-index), or remove a stale index by hand (see the SQL Server migration runbook, section 5a)"]
         :exit   1}
        (migrate-with-dependents [] t-a-index t-a-default [(dropping :t :a)]))

;; (b) After an earlier {:drop-index ix_a :on t} the command is accepted: no IF EXISTS and
;; no DROP INDEX statement, and the default still goes in the column's statement.
(expect {:sql [column-and-default] :errors [] :exit nil}
        (migrate-with-dependents [{:drop-index :ix_a :on :t}] t-a-index t-a-default [(dropping :t :a)]))

;; (c) The command's own :drop-index key naming the index: accepted, the key's own statement
;; first.
(expect {:sql ["drop index if exists ix_a on t" column-and-default] :errors [] :exit nil}
        (migrate-with-dependents [] t-a-index t-a-default [(assoc (dropping :t :a) :drop-index [:ix_a])]))

;; (d) After an earlier :drop-table of t, which drops every index on it: accepted.
(expect {:sql [column-and-default] :errors [] :exit nil}
        (migrate-with-dependents [{:drop-table :t}] t-a-index t-a-default [(dropping :t :a)]))

;; (e) An index on another column of the table does not depend on a: accepted.
(expect {:sql [column-and-default] :errors [] :exit nil}
        (migrate-with-dependents [] {["t" "b"] ["ix_b"]} t-a-default [(dropping :t :a)]))

;; An earlier :alter-table's :drop-index key on t drops the index too: accepted.
(expect {:sql [column-and-default] :errors [] :exit nil}
        (migrate-with-dependents [{:alter-table :t :drop-index [:ix_a]}] t-a-index t-a-default [(dropping :t :a)]))

(def ^:private refused-ix-a
  {:sql [] :errors [(refusal "t" "a" "ix_a")] :exit 1})

;; Only a drop of that index on that table counts: the index dropped on another table (in
;; either form), a :drop-index that names no table, a :drop-table of another table, or a
;; :create-index of the index (its guard skips it while it exists) leaves the refusal.
(expect [refused-ix-a refused-ix-a refused-ix-a refused-ix-a refused-ix-a]
        (vec (for [plan [[{:drop-index :ix_a :on :u}]
                         [{:alter-table :u :drop-index [:ix_a]}]
                         [{:drop-index :ix_a}]
                         [{:drop-table :u}]
                         [{:create-index :ix_a :on :t :columns [:b]}]]]
               (migrate-with-dependents plan t-a-index t-a-default [(dropping :t :a)]))))

;; The plan's drops and the command's own key are compared with the catalog as catalog
;; names: a kebab-case index or table, or one written as a string, is the snake_case one the
;; catalog holds.
(expect [{:sql ["ALTER TABLE my_table DROP column my_col"] :errors [] :exit nil}
         {:sql ["ALTER TABLE my_table DROP column my_col"] :errors [] :exit nil}
         {:sql ["drop index if exists my_idx on my_table" "ALTER TABLE my_table DROP column my_col"] :errors [] :exit nil}]
        (vec (for [[plan command] [[[{:drop-index :my-idx :on :my-table}] (dropping :my_table :my-col)]
                                   [[{:drop-index "my_idx" :on "my_table"}] (dropping :my-table :my_col)]
                                   [[] (assoc (dropping :my_table :my_col) :drop-index [:my-idx])]]]
               (migrate-with-dependents plan {["my_table" "my_col"] ["my_idx"]} {} [command]))))

;; Refused, the line names a kebab-spelled table and column as the catalog holds them.
(expect {:sql    []
         :errors ["tern: cannot drop column my_table.my_col on SQL Server: index my_idx still contains it; drop the index first in this migration (:drop-index), or remove a stale index by hand (see the SQL Server migration runbook, section 5a)"]
         :exit   1}
        (migrate-with-dependents [] {["my_table" "my_col"] ["my_idx"]} {} [(dropping :my-table :my-col)]))

(def ^:private user-index {["user" "name"] ["user_x_idx"]})

;; A reserved-word table: the line names it as the catalog holds it, user, and a drop of the
;; index on :user counts; statements still bracket it.
(expect [{:sql [] :errors [(refusal "user" "name" "user_x_idx")] :exit 1}
         {:sql ["ALTER TABLE [user] DROP column name"] :errors [] :exit nil}]
        [(migrate-with-dependents [] user-index {} [(dropping :user :name)])
         (migrate-with-dependents [{:drop-index :user_x_idx :on :user}] user-index {} [(dropping :user :name)])])

(def ^:private two-indexes
  {["pending_updates" "lease_expires_at"] ["pending_updates_lease_idx" "pending_updates_other_idx"]})

(def ^:private refused-other-idx
  {:sql [] :errors [(refusal "pending_updates" "lease_expires_at" "pending_updates_other_idx")] :exit 1})

;; Each index is checked on its own: with two on the column and one dropped earlier, the
;; other is refused by name ...
(expect refused-other-idx
        (migrate-with-dependents [{:drop-index :pending_updates_lease_idx :on :pending_updates}]
                                 two-indexes {} [(dropping :pending_updates :lease_expires_at)]))

;; ... and so it is when the command's own :drop-index key names the first: the refused
;; command generates nothing, its own index drop included.
(expect refused-other-idx
        (migrate-with-dependents [] two-indexes {}
                                 [(assoc (dropping :pending_updates :lease_expires_at)
                                         :drop-index [:pending_updates_lease_idx])]))

;; One line per dropped column and index that still contains it, then one exit: ix_ab holds
;; both dropped columns, ix_b only b, and ix_c a column the command keeps.
(expect {:sql [] :errors [(refusal "t" "a" "ix_ab") (refusal "t" "b" "ix_ab") (refusal "t" "b" "ix_b")] :exit 1}
        (migrate-with-dependents [] {["t" "a"] ["ix_ab"] ["t" "b"] ["ix_ab" "ix_b"] ["t" "c"] ["ix_c"]} {}
                                 [(dropping :t :a :b)]))

(def ^:private lease-indexes  {["pending_updates" "lease_expires_at"] ["pending_updates_lease_idx"]})
(def ^:private lease-defaults {["pending_updates" "lease_owner"] ["DF_pending_updates_lease_owner"]})

;; The real shape (i2kdb 20260816100000 drops pending_updates_lease_idx, then lease_owner and
;; lease_expires_at): without its :drop-index the column drop is refused; with it, as the
;; migration has it, the index goes by that command, and the default and both columns in one
;; ALTER TABLE, which SQL Server applies whole or not at all.
(expect [{:sql [] :errors [(refusal "pending_updates" "lease_expires_at" "pending_updates_lease_idx")] :exit 1}
         {:sql    ["DROP INDEX pending_updates_lease_idx ON pending_updates"
                   "ALTER TABLE pending_updates DROP constraint if exists DF_pending_updates_lease_owner, column lease_owner,lease_expires_at"]
          :errors []
          :exit   nil}]
        [(migrate-with-dependents [] lease-indexes lease-defaults
                                  [(dropping :pending_updates :lease_owner :lease_expires_at)])
         (migrate-with-dependents [] lease-indexes lease-defaults
                                  [{:drop-index :pending_updates_lease_idx :on :pending_updates}
                                   (dropping :pending_updates :lease_owner :lease_expires_at)])])

;; With nothing depending on the columns, only the column drop.
(expect ["ALTER TABLE pending_updates DROP column lease_owner,lease_expires_at"]
        (:sql (migrate-with-dependents [] {} {} [(dropping :pending_updates :lease_owner :lease_expires_at)])))

;; An index dropped by the command's own key and created again later in the migration, on a
;; column the command keeps, is created: the command, in the plan once generated, drops it.
(expect {:sql ["drop index if exists ix_a on t" column-and-default "CREATE  INDEX ix_a ON t (b)"] :errors [] :exit nil}
        (migrate-with-dependents [] t-a-index t-a-default
                                 [(assoc (dropping :t :a) :drop-index [:ix_a])
                                  {:create-index :ix_a :on :t :columns [:b]}]))

(defn- run-migration-offline
  "tern's own run-migration!, through SqlServerMigrator's migrate (version table
  schema_versions), over fake-catalog: the connection is a stand-in that records
  that it was opened, and each statement is recorded instead of executed.  Returns
  {:connected? :executed :errors :exit}."
  [indexes defaults commands]
  (let [connected (atom false)
        executed  (atom [])
        errors    (atom [])
        exit      (atom nil)]
    (with-redefs-fn (merge (fake-catalog indexes defaults exit errors)
                           {#'jdbc/get-connection          (fn [& _]
                                                             (reset! connected true)
                                                             (reify java.sql.Connection (close [_])))
                            #'sqlserver/execute-statement! (fn [_ sql] (swap! executed conj sql))})
      (fn []
        (try
          (tern-db/migrate (sqlserver/->SqlServerMigrator {:db db :version-table "schema_versions"})
                           "20261015000000" commands)
          (catch clojure.lang.ExceptionInfo e
            (when-not (exit-thrown? e) (throw e))))))
    {:connected? @connected :executed @executed :errors @errors :exit @exit}))

(def ^:private a-in-two-indexes {["t" "a"] ["ix_a" "ix_a2"]})

;; The refusal comes while the migration's SQL is generated, before its first statement runs.
;; Through tern's own run-migration!, a migration that drops ix_a and then a, which ix_a2
;; still contains, opens no connection and executes nothing, its first command's DROP INDEX
;; included, so no version row is written; the same migration dropping both indexes first
;; executes every statement and its version row.
(expect [{:connected? false :executed [] :errors [(refusal "t" "a" "ix_a2")] :exit 1}
         {:connected? true
          :executed   ["DROP INDEX ix_a ON t"
                       "DROP INDEX ix_a2 ON t"
                       column-and-default
                       "INSERT INTO schema_versions (version,created) VALUES (20261015000000,CURRENT_TIMESTAMP)"]
          :errors     []
          :exit       nil}]
        [(run-migration-offline a-in-two-indexes t-a-default
                                [{:drop-index :ix_a :on :t} (dropping :t :a)])
         (run-migration-offline a-in-two-indexes t-a-default
                                [{:drop-index :ix_a :on :t} {:drop-index :ix_a2 :on :t} (dropping :t :a)])])

;; Only a drop earlier in the migration counts: one after the column drop comes too late,
;; so the column drop is refused and nothing of the migration runs.
(expect {:connected? false :executed [] :errors [(refusal "t" "a" "ix_a")] :exit 1}
        (run-migration-offline t-a-index t-a-default [(dropping :t :a) {:drop-index :ix_a :on :t}]))

;; ---------------------------------------------------------------------------
;; An earlier command with a :sqlserver override runs the override instead of its
;; structured form, so it counts as dropping an index exactly when one of the override's
;; statements is DROP INDEX [IF EXISTS] <index> ON <table>.

(defn- overriding
  "A command whose structured form drops nothing, with `sql` as its :sqlserver override."
  [sql]
  {:update "UPDATE t SET a = a WHERE 1 = 0" :sqlserver sql})

(defn- accepted-after
  "The outcome of an earlier command's `statements` followed by an accepted drop of
  t.a (ix_a, DF_t_a): those statements, then the column with its default."
  [& statements]
  {:sql (conj (vec statements) column-and-default) :errors [] :exit nil})

(defn- refused-after
  "The outcome of an earlier command's `statements` followed by a refused drop of t.a."
  [& statements]
  {:sql (vec statements) :errors [(refusal "t" "a" "ix_a")] :exit 1})

(defn- after-override
  [sql]
  (migrate-with-dependents [] t-a-index t-a-default [(overriding sql) (dropping :t :a)]))

;; tern's own lowercase form in a vector counts ...
(expect (accepted-after "drop index if exists ix_a on t")
        (after-override ["drop index if exists ix_a on t"]))

;; ... and so does the upper-case form ...
(expect (accepted-after "DROP INDEX ix_a ON t")
        (after-override ["DROP INDEX ix_a ON t"]))

;; ... with names bracketed or double-quoted, a schema-qualified table (its last segment
;; is the table), keywords in mixed case, other whitespace and a closing semicolon.
(expect [(accepted-after "DROP INDEX [ix_a] ON [t]")
         (accepted-after "DROP INDEX ix_a ON dbo.t")
         (accepted-after "DROP INDEX \"ix_a\" ON [dbo].[t]")
         (accepted-after "Drop Index If Exists ix_a\n\tOn t;")]
        [(after-override ["DROP INDEX [ix_a] ON [t]"])
         (after-override ["DROP INDEX ix_a ON dbo.t"])
         (after-override ["DROP INDEX \"ix_a\" ON [dbo].[t]"])
         (after-override ["Drop Index If Exists ix_a\n\tOn t;"])])

;; Every statement of a vector override is read, not only the first ...
(expect (accepted-after "UPDATE t SET a = NULL WHERE 1 = 0" "DROP INDEX ix_a ON t")
        (after-override ["UPDATE t SET a = NULL WHERE 1 = 0" "DROP INDEX ix_a ON t"]))

;; ... and an override given as a string is read as one statement.
(expect (accepted-after "drop index ix_a on t")
        (after-override "drop index ix_a on t"))

;; A nested vector is read at every depth, as run-migration! flattens it (tern.db/sql-statements).
(expect (accepted-after "DROP INDEX ix_a ON t")
        (after-override [["DROP INDEX ix_a ON t"]]))

;; An override that drops the index on another table, or drops another index, or only
;; mentions a drop of it inside a string literal, drops nothing here: still refused.
(expect [(refused-after "DROP INDEX ix_a ON u")
         (refused-after "DROP INDEX ix_b ON t")
         (refused-after "INSERT INTO notes (body) VALUES ('drop index ix_a on t; done')")]
        [(after-override ["DROP INDEX ix_a ON u"])
         (after-override ["DROP INDEX ix_b ON t"])
         (after-override ["INSERT INTO notes (body) VALUES ('drop index ix_a on t; done')"])])

;; A statement counts only if it starts with DROP INDEX: a drop behind a condition that may
;; not hold, or inside a comment, drops nothing here: still refused.
(expect [(refused-after "IF 1 = 0 DROP INDEX ix_a ON t")
         (refused-after "-- DROP INDEX ix_a ON t")
         (refused-after "/* DROP INDEX ix_a ON t */")]
        [(after-override ["IF 1 = 0 DROP INDEX ix_a ON t"])
         (after-override ["-- DROP INDEX ix_a ON t"])
         (after-override ["/* DROP INDEX ix_a ON t */"])])

;; A command with an override counts only by what its override drops, not by its structured
;; :drop-index, which does not run: an empty override, or one that drops nothing, leaves the
;; refusal.
(expect {:sql [] :errors [(refusal "t" "a" "ix_a")] :exit 1}
        (migrate-with-dependents [] t-a-index t-a-default
                                 [{:drop-index :ix_a :on :t :sqlserver []} (dropping :t :a)]))

(expect {:sql ["UPDATE t SET a = a WHERE 1 = 0"] :errors [(refusal "t" "a" "ix_a")] :exit 1}
        (migrate-with-dependents [] t-a-index t-a-default
                                 [{:drop-index :ix_a :on :t :sqlserver ["UPDATE t SET a = a WHERE 1 = 0"]}
                                  (dropping :t :a)]))

;; The create guard reads the same drops: after an override that drops ix_a, a structured
;; :create-index of ix_a is emitted though the catalog still shows it; after one that drops
;; another index it is skipped.
(expect [{:sql ["DROP INDEX ix_a ON t" "CREATE  INDEX ix_a ON t (b)"] :errors [] :exit nil}
         {:sql ["DROP INDEX ix_b ON t"] :errors [] :exit nil}]
        [(migrate-with-dependents [] t-a-index {} [(overriding "DROP INDEX ix_a ON t")
                                                   {:create-index :ix_a :on :t :columns [:b]}])
         (migrate-with-dependents [] t-a-index {} [(overriding "DROP INDEX ix_b ON t")
                                                   {:create-index :ix_a :on :t :columns [:b]}])])

;; The real shape (i2kdb 20160607135101): the index is dropped by the override under its
;; SQL Server name, while the structured :drop-index names MySQL's; the column drop is
;; accepted, the override's statement first.
(expect {:sql    ["drop index if exists idx_taxnodes_config_client_key on taxnodes_config"
                  "ALTER TABLE taxnodes_config DROP column clients_id"]
         :errors []
         :exit   nil}
        (migrate-with-dependents [] {["taxnodes_config" "clients_id"] ["idx_taxnodes_config_client_key"]} {}
                                 [{:drop-index :idx_taxnodes_config_client_path_key :on :taxnodes_config
                                   :sqlserver ["drop index if exists idx_taxnodes_config_client_key on taxnodes_config"]}
                                  {:alter-table :taxnodes_config
                                   :drop-columns [:clients_id]}]))

;; ---------------------------------------------------------------------------
;; Tables and constraints are read the same way: an earlier override's DROP TABLE
;; statement drops that table, its ALTER TABLE ... DROP CONSTRAINT statement the
;; constraints it names.  For the column-drop refusal a command with an override counts
;; only by them, not by its :drop-table or :drop-constraints key, which does not run; for
;; the guards (create-table, create-index, add-constraints) its keys count as well, so a
;; drop the override performs in a form not read never leaves a create or an add silently
;; skipped: at worst the create or add fails when it runs.

;; A DROP TABLE of t in an override drops t's indexes, in the forms a DROP INDEX statement
;; may take: the column drop is accepted.  A DROP TABLE of another table, or one behind a
;; condition, drops nothing of t: still refused.
(expect [(accepted-after "DROP TABLE t")
         (accepted-after "drop table if exists [dbo].[t];")
         (accepted-after "Drop Table \"t\"")
         (refused-after "DROP TABLE u")
         (refused-after "IF OBJECT_ID('t') IS NOT NULL DROP TABLE t")]
        [(after-override ["DROP TABLE t"])
         (after-override ["drop table if exists [dbo].[t];"])
         (after-override "Drop Table \"t\"")
         (after-override ["DROP TABLE u"])
         (after-override ["IF OBJECT_ID('t') IS NOT NULL DROP TABLE t"])])

;; For the refusal a :drop-table of t with an override counts only by the override: an empty
;; one, or one that drops another table, leaves t's indexes (refused); one that drops t counts.
(expect [{:sql [] :errors [(refusal "t" "a" "ix_a")] :exit 1}
         {:sql ["DROP TABLE u"] :errors [(refusal "t" "a" "ix_a")] :exit 1}
         {:sql ["DROP TABLE t" column-and-default] :errors [] :exit nil}]
        (vec (for [override [[] ["DROP TABLE u"] ["DROP TABLE t"]]]
               (migrate-with-dependents [] t-a-index t-a-default
                                        [{:drop-table :t :sqlserver override} (dropping :t :a)]))))

;; The create-table guard counts the key as well: after a :drop-table of t whose override drops
;; another table, CREATE TABLE t is emitted (and fails when it runs if t is still there);
;; after another command whose override drops another table it is skipped while the catalog
;; shows t; after one whose override drops t it is emitted.
(expect [[nil ["CREATE TABLE t (a INT)"]] [nil nil] [nil ["CREATE TABLE t (a INT)"]]]
        [(create-table-after [{:drop-table :t :sqlserver ["DROP TABLE u"]}])
         (create-table-after [(overriding "DROP TABLE u")])
         (create-table-after [(overriding "DROP TABLE [t]")])])

;; An ALTER TABLE ... DROP CONSTRAINT of fk_x on t in an override drops it, in the forms a
;; DROP INDEX statement may take, behind a condition too (the guards read a drop anywhere in
;; a statement), so the add-constraints guard adds it again.  One that drops another
;; constraint, or fk_x on another table, drops nothing here: the guard skips the add while
;; the catalog shows fk_x.
(expect [[[] ["ALTER TABLE t ADD CONSTRAINT fk_x FOREIGN KEY (a) REFERENCES u(id)"]]
         [[] ["ALTER TABLE t ADD CONSTRAINT fk_x FOREIGN KEY (a) REFERENCES u(id)"]]
         [[] []]
         [[] []]
         [[] ["ALTER TABLE t ADD CONSTRAINT fk_x FOREIGN KEY (a) REFERENCES u(id)"]]]
        [(add-constraint-after [(overriding "ALTER TABLE t DROP CONSTRAINT fk_x")] :t)
         (add-constraint-after [(overriding ["alter table [dbo].[t] drop constraint if exists \"fk_x\";"])] :t)
         (add-constraint-after [(overriding "ALTER TABLE t DROP CONSTRAINT fk_y")] :t)
         (add-constraint-after [(overriding "ALTER TABLE u DROP CONSTRAINT fk_x")] :t)
         (add-constraint-after [(overriding "IF OBJECT_ID('fk_x') IS NOT NULL ALTER TABLE t DROP CONSTRAINT fk_x")] :t)])

;; Every constraint of the statement's list counts: both foreign keys are added again.  A
;; statement naming only fk_a adds back only fk_a.
(expect [[[] ["ALTER TABLE t ADD CONSTRAINT fk_a FOREIGN KEY (a) REFERENCES u(id),CONSTRAINT fk_b FOREIGN KEY (a) REFERENCES u(id)"]]
         [[] ["ALTER TABLE t ADD CONSTRAINT fk_a FOREIGN KEY (a) REFERENCES u(id)"]]]
        [(add-constraint-after [(overriding "ALTER TABLE t DROP CONSTRAINT fk_a, [fk_b]")] :t [:fk_a :fk_b] #{"fk_a" "fk_b"})
         (add-constraint-after [(overriding "ALTER TABLE t DROP CONSTRAINT fk_a")] :t [:fk_a :fk_b] #{"fk_a" "fk_b"})])

;; For the add-constraints guard an :alter-table of t with an override counts by its
;; :drop-constraints key as well: after an empty override, one that drops a trigger, or one
;; that drops fk_x, fk_x is added again (and fails when it runs if the override left it).
(expect [[[] ["ALTER TABLE t ADD CONSTRAINT fk_x FOREIGN KEY (a) REFERENCES u(id)"]]
         [[] ["ALTER TABLE t ADD CONSTRAINT fk_x FOREIGN KEY (a) REFERENCES u(id)"]]
         [[] ["ALTER TABLE t ADD CONSTRAINT fk_x FOREIGN KEY (a) REFERENCES u(id)"]]]
        [(add-constraint-after [{:alter-table :t :drop-constraints [:fk_x] :sqlserver []}] :t)
         (add-constraint-after [{:alter-table :t :drop-constraints [:fk_x] :sqlserver ["drop trigger if exists x"]}] :t)
         (add-constraint-after [{:alter-table :t :drop-constraints [:fk_x] :sqlserver ["ALTER TABLE t DROP CONSTRAINT fk_x"]}] :t)])

;; The guards read a drop behind a condition, in the house style of i2k's overrides, as a
;; drop: after an override that drops fk_x if it exists, fk_x is added again, and after one
;; that drops t if it exists, CREATE TABLE t is emitted; a drop counted too often there at
;; worst makes the add or the create fail when it runs, never skips it without a word.
(expect [[[] ["ALTER TABLE t ADD CONSTRAINT fk_x FOREIGN KEY (a) REFERENCES u(id)"]]
         [nil ["CREATE TABLE t (a INT)"]]]
        [(add-constraint-after [(overriding ["IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_x') ALTER TABLE t DROP CONSTRAINT fk_x"])] :t)
         (create-table-after [(overriding ["IF OBJECT_ID('t') IS NOT NULL DROP TABLE t"])])])

;; The refusal does not: after an override that drops ix_a if it exists, the create-index
;; guard emits CREATE INDEX ix_a, while a drop of t.a, which ix_a contains, is still refused.
(expect [{:sql ["IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_a') DROP INDEX ix_a ON t" "CREATE  INDEX ix_a ON t (b)"] :errors [] :exit nil}
         (refused-after "IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_a') DROP INDEX ix_a ON t")]
        [(migrate-with-dependents [] t-a-index {} [(overriding ["IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_a') DROP INDEX ix_a ON t"])
                                                   {:create-index :ix_a :on :t :columns [:b]}])
         (after-override ["IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_a') DROP INDEX ix_a ON t"])])

;; The override readers fold too: a DROP TABLE T lets CREATE TABLE t through, and an ALTER
;; TABLE T DROP CONSTRAINT FK_X has fk_x added again.
(expect [[nil ["CREATE TABLE t (a INT)"]]
         [[] ["ALTER TABLE t ADD CONSTRAINT fk_x FOREIGN KEY (a) REFERENCES u(id)"]]]
        [(create-table-after [(overriding "DROP TABLE T")])
         (add-constraint-after [(overriding "ALTER TABLE T DROP CONSTRAINT FK_X")] :t)])

(defn- migrate-over-fks
  "migrate-with-dependents with no index or default on any column and a catalog whose
  foreign-key lookup finds exactly `fks`, answering only for the bound *db*."
  [fks commands]
  (with-redefs [sqlserver/foreign-key-exists? (fn [d c] (and (= d db) (contains? fks c)))]
    (migrate-with-dependents [] {} {} commands)))

(def ^:private taxnodes-down
  "i2kdb 20160603031901-cascade_taxonomy_and_taxnode_deletes.edn's :down, its first two
  commands: on SQL Server the first runs only its override, which drops a trigger and
  leaves fk_taxnodes_parent_id_cd in place, though its key claims the drop; the second
  adds that constraint."
  [{:alter-table :taxnodes
    :drop-constraints [:fk_taxnodes_parent_id_cd]
    :sqlserver ["drop trigger if exists delete_taxnode"]}
   {:alter-table :taxnodes
    :add-constraints [[:fk_taxnodes_parent_id_cd "(parent_id) REFERENCES taxnodes(id)"]]}])

;; The real shape: over a catalog that shows fk_taxnodes_parent_id_cd, the first command's key
;; claims the drop, so the guard emits the add, which fails when it runs (the override left
;; the constraint) rather than leaving the cascade key in place without a word; nothing is
;; refused ...
(expect {:sql ["drop trigger if exists delete_taxnode" "ALTER TABLE taxnodes ADD CONSTRAINT fk_taxnodes_parent_id_cd FOREIGN KEY (parent_id) REFERENCES taxnodes(id)"] :errors [] :exit nil}
        (migrate-over-fks #{"fk_taxnodes_parent_id_cd"} taxnodes-down))

;; ... and with an override that drops the constraint too, the add is emitted and runs.
(expect {:sql    ["ALTER TABLE taxnodes DROP CONSTRAINT fk_taxnodes_parent_id_cd" "drop trigger if exists delete_taxnode" "ALTER TABLE taxnodes ADD CONSTRAINT fk_taxnodes_parent_id_cd FOREIGN KEY (parent_id) REFERENCES taxnodes(id)"]
         :errors []
         :exit   nil}
        (migrate-over-fks #{"fk_taxnodes_parent_id_cd"}
                          (assoc-in taxnodes-down [0 :sqlserver]
                                    ["ALTER TABLE taxnodes DROP CONSTRAINT fk_taxnodes_parent_id_cd"
                                     "drop trigger if exists delete_taxnode"])))

;; The two readings side by side: a :drop-table of t whose override drops t behind a condition,
;; a form not read, lets a later CREATE TABLE t through, as the guard counts the key's claim;
;; a later drop of t.a, which ix_a contains, is still refused, as the refusal counts only what
;; the override is read to drop.
(expect [["IF OBJECT_ID('t') IS NOT NULL DROP TABLE t" "CREATE TABLE t (a INT)"]
         {:sql ["IF OBJECT_ID('t') IS NOT NULL DROP TABLE t"] :errors [(refusal "t" "a" "ix_a")] :exit 1}]
        [(migrate-over-catalog {:tables #{"t"}}
                               [{:drop-table :t :sqlserver ["IF OBJECT_ID('t') IS NOT NULL DROP TABLE t"]}
                                {:create-table :t :columns [[:a "INT"]]}])
         (migrate-with-dependents [] t-a-index t-a-default
                                  [{:drop-table :t :sqlserver ["IF OBJECT_ID('t') IS NOT NULL DROP TABLE t"]}
                                   (dropping :t :a)])])

;; ---------------------------------------------------------------------------
;; An :alter-table's :drop-index key may name a single index as a keyword: its own statement
;; drops that index, and the column drop counts it ...
(expect [["drop index if exists x on t"]
         {:sql ["drop index if exists ix_a on t" column-and-default] :errors [] :exit nil}]
        [(vec (sqlserver/generate-sql {:alter-table :t :drop-index :x}))
         (migrate-with-dependents [] t-a-index t-a-default [(assoc (dropping :t :a) :drop-index :ix_a)])])

;; ... and only that index: a keyword naming another index leaves the refusal.
(expect refused-ix-a
        (migrate-with-dependents [] t-a-index t-a-default [(assoc (dropping :t :a) :drop-index :ix_b)]))

;; A vector keeps every index it names, and a string names one index.
(expect [["drop index if exists x on t,y on t"] ["drop index if exists x on t"]]
        [(vec (sqlserver/generate-sql {:alter-table :t :drop-index [:x :y]}))
         (vec (sqlserver/generate-sql {:alter-table :t :drop-index "x"}))])

;; ---------------------------------------------------------------------------
;; The plan's names are compared with the catalog's folded: SQL Server's default collation
;; does not tell identifiers apart by case, and neither do its OBJECT_ID and indexproperty.

;; The refusal: an index spelled IX_A in the migration and ix_a in the catalog, or ix_a in the
;; migration and IX_A in the catalog, or the table spelled T, is the one dropped; so is the
;; index of an upper-case DROP INDEX in an override, and t's after a :drop-table of T.
(expect [{:sql ["drop index if exists IX_A on t" column-and-default] :errors [] :exit nil}
         {:sql ["drop index if exists ix_a on t" column-and-default] :errors [] :exit nil}
         {:sql ["drop index if exists ix_a on T" column-and-default] :errors [] :exit nil}
         (accepted-after "DROP INDEX IX_A ON T")
         {:sql [column-and-default] :errors [] :exit nil}]
        [(migrate-with-dependents [] t-a-index t-a-default [{:alter-table :t :drop-index [:IX_A]} (dropping :t :a)])
         (migrate-with-dependents [] {["t" "a"] ["IX_A"]} t-a-default [{:alter-table :t :drop-index [:ix_a]} (dropping :t :a)])
         (migrate-with-dependents [] t-a-index t-a-default [{:alter-table :T :drop-index [:ix_a]} (dropping :t :a)])
         (after-override ["DROP INDEX IX_A ON T"])
         (migrate-with-dependents [{:drop-table :T}] t-a-index t-a-default [(dropping :t :a)])])

;; Folding makes no other name match: a drop of IX_B leaves IX_A, and the line names the
;; index as the catalog holds it.
(expect {:sql ["drop index if exists IX_B on t"] :errors [(refusal "t" "a" "IX_A")] :exit 1}
        (migrate-with-dependents [] {["t" "a"] ["IX_A"]} t-a-default [{:alter-table :t :drop-index [:IX_B]} (dropping :t :a)]))

;; The guards compare the same way: CREATE INDEX x is emitted after a drop of X on T or a
;; :drop-table of T, CREATE TABLE t after a :drop-table of T, and fk_x is added again after
;; a :drop-constraints of FK_X on T.
(expect [[nil ["CREATE  INDEX x ON t (a)"]]
         [nil ["CREATE  INDEX x ON t (a)"]]
         [nil ["CREATE TABLE t (a INT)"]]
         [[] ["ALTER TABLE t ADD CONSTRAINT fk_x FOREIGN KEY (a) REFERENCES u(id)"]]]
        [(create-after [{:drop-index :X :on :T}] :x)
         (create-after [{:drop-table :T}] :x)
         (create-table-after [{:drop-table :T}])
         (add-constraint-after [{:alter-table :T :drop-constraints [:FK_X]}] :t)])

;; What folding gets wrong on a database whose collation is case-sensitive, where ix_a and IX_A
;; are two indexes: a drop of ix_a counts for both, so the column drop is accepted here and
;; fails when it runs (error 5074); with neither dropped, the line names only ix_a.  (A single
;; name spelled in another case than the catalog's, as in the cases above, is the same fault
;; there.)
(expect [{:sql ["drop index if exists ix_a on t" "ALTER TABLE t DROP column a"] :errors [] :exit nil}
         {:sql [] :errors [(refusal "t" "a" "ix_a")] :exit 1}]
        [(migrate-with-dependents [] {["t" "a"] ["ix_a" "IX_A"]} {} [(assoc (dropping :t :a) :drop-index [:ix_a])])
         (migrate-with-dependents [] {["t" "a"] ["ix_a" "IX_A"]} {} [(dropping :t :a)])])

;; Names fold in the root locale: under a Turkish default locale IX_A still folds to ix_a, not
;; to a dotless i.  (folded-name is looked up when the test runs, so this file still loads
;; against an older generator that lacks it.)
(expect "ix_a"
        (let [folded-name (ns-resolve 'tern.sqlserver 'folded-name)
              default     (java.util.Locale/getDefault)]
          (try (java.util.Locale/setDefault (java.util.Locale. "tr" "TR"))
               (folded-name :IX_A)
               (finally (java.util.Locale/setDefault default)))))

;; ---------------------------------------------------------------------------
;; An index an earlier command of the migration creates is not in the catalog, which is read
;; before anything runs, but it is on the table when the column drop runs: the refusal counts
;; it too, by its key and included columns, and names it in the same line.

(defn- refused-by
  "The outcome of earlier commands' `statements` followed by a drop of t.a that `index`
  still contains."
  [index & statements]
  {:sql (vec statements) :errors [(refusal "t" "a" index)] :exit 1})

;; Through tern's own run-migration!: a :create-index on a and then the drop of a opens no
;; connection and executes nothing; with the index created on b instead, the migration runs
;; whole.
(expect [{:connected? false :executed [] :errors [(refusal "t" "a" "ix_n")] :exit 1}
         {:connected? true
          :executed   ["CREATE  INDEX ix_n ON t (b)"
                       column-and-default
                       "INSERT INTO schema_versions (version,created) VALUES (20261015000000,CURRENT_TIMESTAMP)"]
          :errors     []
          :exit       nil}]
        [(run-migration-offline {} t-a-default [{:create-index :ix_n :on :t :columns [:a]} (dropping :t :a)])
         (run-migration-offline {} t-a-default [{:create-index :ix_n :on :t :columns [:b]} (dropping :t :a)])])

;; A key column written with DESC is the column itself, as in an override's CREATE INDEX.
(expect (refused-by "ix_n" "CREATE  INDEX ix_n ON t (a DESC)")
        (migrate-with-dependents [] {} t-a-default [{:create-index :ix_n :on :t :columns ["a DESC"]} (dropping :t :a)]))

;; Any column of the :create-index counts, also with a prefix length; the table, index and
;; columns compare folded, kebab-case or in another case; the line names the index as its
;; catalog name.
(expect [(refused-by "ix_n" "CREATE  INDEX ix_n ON t (b, a)")
         (refused-by "ix_n" "CREATE  INDEX ix_n ON t (a)")
         {:sql ["CREATE  INDEX ix_n ON MY_TABLE (My_Col)"] :errors [(refusal "my_table" "my_col" "ix_n")] :exit 1}]
        [(migrate-with-dependents [] {} {} [{:create-index :ix_n :on :t :columns [:b :a]} (dropping :t :a)])
         (migrate-with-dependents [] {} {} [{:create-index :ix_n :on :t :columns ["a(10)"]} (dropping :t :a)])
         (migrate-with-dependents [] {} {} [{:create-index :ix-n :on :MY-TABLE :columns [:My-Col]} (dropping :my_table :my_col)])])

;; An index created on another table does not contain t.a: accepted.
(expect (accepted-after "CREATE  INDEX ix_n ON u (a)")
        (migrate-with-dependents [] {} t-a-default [{:create-index :ix_n :on :u :columns [:a]} (dropping :t :a)]))

(defn- after-create-override
  "An earlier command whose override is `sql`, then a drop of t.a, which no catalog index
  contains (its default is DF_t_a)."
  [sql]
  (migrate-with-dependents [] {} t-a-default [(overriding sql) (dropping :t :a)]))

;; A CREATE INDEX statement in an override counts by its key and included columns, in the
;; forms a DROP INDEX statement may take, with UNIQUE, CLUSTERED or NONCLUSTERED, and ASC or
;; DESC on a key column ...
(expect [(refused-by "ix_n" "CREATE INDEX ix_n ON t (a)")
         (refused-by "ix_n" "create unique nonclustered index [ix_n] on [dbo].[t] (b DESC, \"a\" ASC);")
         (refused-by "ix_n" "CREATE CLUSTERED INDEX ix_n ON t(b) INCLUDE (c, a)")
         (refused-by "IX_N" "CREATE INDEX IX_N ON T (A)")]
        [(after-create-override "CREATE INDEX ix_n ON t (a)")
         (after-create-override ["create unique nonclustered index [ix_n] on [dbo].[t] (b DESC, \"a\" ASC);"])
         (after-create-override ["CREATE CLUSTERED INDEX ix_n ON t(b) INCLUDE (c, a)"])
         (after-create-override ["CREATE INDEX IX_N ON T (A)"])])

;; ... and one over other columns, or on another table, does not contain t.a: accepted.
(expect [(accepted-after "CREATE INDEX ix_n ON t (b) INCLUDE (c)")
         (accepted-after "CREATE INDEX ix_n ON u (a)")]
        [(after-create-override ["CREATE INDEX ix_n ON t (b) INCLUDE (c)"])
         (after-create-override ["CREATE INDEX ix_n ON u (a)"])])

;; Creates are read generously, as one counted too often only refuses a column drop: a CREATE
;; INDEX clause anywhere in a statement counts, behind IF NOT EXISTS (the repair migrations'
;; form), before WHERE, WITH or ON <filegroup>, with COLUMNSTORE, and so does one in an
;; :update, which runs as written ...
(expect [(refused-by "ix_n" "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_n' AND object_id = OBJECT_ID('t')) CREATE INDEX ix_n ON t (a)")
         (refused-by "ix_n" "CREATE INDEX ix_n ON t (a) WHERE a IS NOT NULL")
         (refused-by "ix_n" "CREATE INDEX ix_n ON t (b) INCLUDE (a) WITH (ONLINE = ON) ON [PRIMARY]")
         (refused-by "ix_n" "CREATE NONCLUSTERED COLUMNSTORE INDEX ix_n ON t (a)")
         (refused-by "ix_n" "CREATE INDEX ix_n ON t (a)")]
        [(after-create-override ["IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_n' AND object_id = OBJECT_ID('t')) CREATE INDEX ix_n ON t (a)"])
         (after-create-override ["CREATE INDEX ix_n ON t (a) WHERE a IS NOT NULL"])
         (after-create-override ["CREATE INDEX ix_n ON t (b) INCLUDE (a) WITH (ONLINE = ON) ON [PRIMARY]"])
         (after-create-override ["CREATE NONCLUSTERED COLUMNSTORE INDEX ix_n ON t (a)"])
         (migrate-with-dependents [] {} t-a-default [{:update "CREATE INDEX ix_n ON t (a)"} (dropping :t :a)])])

;; ... still by table and columns: an :update creating ix_n on b, or on u, leaves t.a free.
(expect [(accepted-after "CREATE INDEX ix_n ON t (b)")
         (accepted-after "CREATE INDEX ix_n ON u (a)")]
        [(migrate-with-dependents [] {} t-a-default [{:update "CREATE INDEX ix_n ON t (b)"} (dropping :t :a)])
         (migrate-with-dependents [] {} t-a-default [{:update "CREATE INDEX ix_n ON u (a)"} (dropping :t :a)])])

;; Order is read command by command: the rebuild idiom, one override dropping ix_n and
;; creating it again on a, leaves ix_n on a when the column drop runs (refused); an override
;; that creates ix_n and then drops it again still counts as creating it, a false refusal the
;; README states.
(expect [(refused-by "ix_n" "DROP INDEX ix_n ON t" "CREATE INDEX ix_n ON t (a)")
         (refused-by "ix_n" "CREATE INDEX ix_n ON t (a)" "DROP INDEX ix_n ON t")]
        [(migrate-with-dependents [] {["t" "a"] ["ix_n"]} t-a-default
                                  [(overriding ["DROP INDEX ix_n ON t" "CREATE INDEX ix_n ON t (a)"]) (dropping :t :a)])
         (migrate-with-dependents [] {} t-a-default
                                  [(overriding ["CREATE INDEX ix_n ON t (a)" "DROP INDEX ix_n ON t"]) (dropping :t :a)])])

;; A :create-index with an override creates only what its override creates.
(expect [(accepted-after "CREATE INDEX ix_n ON t (b)")
         (refused-by "ix_n" "CREATE INDEX ix_n ON t (a)")]
        [(migrate-with-dependents [] {} t-a-default [{:create-index :ix_n :on :t :columns [:a] :sqlserver "CREATE INDEX ix_n ON t (b)"}
                                                     (dropping :t :a)])
         (migrate-with-dependents [] {} t-a-default [{:create-index :ix_n :on :t :columns [:b] :sqlserver "CREATE INDEX ix_n ON t (a)"}
                                                     (dropping :t :a)])])

;; An index the migration creates and then drops again before the column drop is not
;; counted: dropped by an :alter-table's :drop-index key, by a DROP INDEX in an override, with
;; the whole table, or by the column drop's own key.
(expect [(accepted-after "CREATE  INDEX ix_n ON t (a)" "drop index if exists ix_n on t")
         (accepted-after "CREATE  INDEX ix_n ON t (a)" "DROP INDEX ix_n ON t")
         (accepted-after "CREATE  INDEX ix_n ON t (a)" "DROP TABLE t")
         (accepted-after "CREATE  INDEX ix_n ON t (a)" "drop index if exists ix_n on t")]
        [(migrate-with-dependents [] {} t-a-default [{:create-index :ix_n :on :t :columns [:a]}
                                                     {:alter-table :t :drop-index [:ix_n]}
                                                     (dropping :t :a)])
         (migrate-with-dependents [] {} t-a-default [{:create-index :ix_n :on :t :columns [:a]}
                                                     (overriding "DROP INDEX ix_n ON t")
                                                     (dropping :t :a)])
         (migrate-with-dependents [] {} t-a-default [{:create-index :ix_n :on :t :columns [:a]}
                                                     {:drop-table :t}
                                                     (dropping :t :a)])
         (migrate-with-dependents [] {} t-a-default [{:create-index :ix_n :on :t :columns [:a]}
                                                     (assoc (dropping :t :a) :drop-index [:ix_n])])])

;; A drop before the create does not drop what the create makes: the catalog's ix_n is
;; dropped first, the new ix_n on a is refused, in one line.
(expect (refused-by "ix_n" "drop index if exists ix_n on t" "CREATE  INDEX ix_n ON t (a)")
        (migrate-with-dependents [] {["t" "a"] ["ix_n"]} t-a-default
                                 [{:alter-table :t :drop-index [:ix_n]}
                                  {:create-index :ix_n :on :t :columns [:a]}
                                  (dropping :t :a)]))

;; A {:drop-index i :on t} command's guard reads the catalog as it was before the migration:
;; where that catalog holds no index of the name, the command emits nothing, the index the
;; migration created stays, and the column drop is refused ...
(expect (refused-by "ix_n" "CREATE  INDEX ix_n ON t (a)")
        (migrate-with-dependents [] {} t-a-default [{:create-index :ix_n :on :t :columns [:a]}
                                                    {:drop-index :ix_n :on :t}
                                                    (dropping :t :a)]))

;; ... while where it holds one (on b here, dropped and created again on a), the command emits
;; its DROP INDEX, which drops the created index: accepted.
(expect (accepted-after "drop index if exists ix_n on t" "CREATE  INDEX ix_n ON t (a)" "DROP INDEX ix_n ON t")
        (migrate-with-dependents [] {["t" "b"] ["ix_n"]} t-a-default [{:alter-table :t :drop-index [:ix_n]}
                                                                      {:create-index :ix_n :on :t :columns [:a]}
                                                                      {:drop-index :ix_n :on :t}
                                                                      (dropping :t :a)]))

;; A :create-index its guard skips creates nothing: the catalog's ix_n on b stays, and t.a is
;; dropped.  Once an earlier command drops ix_n, the guard emits the create, and it counts.
(expect [(accepted-after)
         (refused-by "ix_n" "drop index if exists ix_n on t" "CREATE  INDEX ix_n ON t (a)")]
        [(migrate-with-dependents [] {["t" "b"] ["ix_n"]} t-a-default [{:create-index :ix_n :on :t :columns [:a]}
                                                                      (dropping :t :a)])
         (migrate-with-dependents [] {["t" "b"] ["ix_n"]} t-a-default [{:alter-table :t :drop-index [:ix_n]}
                                                                      {:create-index :ix_n :on :t :columns [:a]}
                                                                      (dropping :t :a)])])

;; An index named twice, by two creates or by the catalog and an override's create, is named
;; once.
(expect [(refused-by "ix_n" "CREATE  INDEX ix_n ON t (a)" "CREATE  INDEX ix_n ON t (a)")
         (refused-by "ix_n" "CREATE INDEX ix_n ON t (a)")]
        [(migrate-with-dependents [] {} t-a-default [{:create-index :ix_n :on :t :columns [:a]}
                                                    {:create-index :ix_n :on :t :columns [:a]}
                                                    (dropping :t :a)])
         (migrate-with-dependents [] {["t" "a"] ["ix_n"]} t-a-default [(overriding "CREATE INDEX ix_n ON t (a)")
                                                                      (dropping :t :a)])])

;; Without a connection nothing is refused: there is no catalog, and the generators emit
;; their statements unguarded.
(expect ["ALTER TABLE t DROP column a"]
        (with-redefs [sqlserver/exit! (fn [_] (throw (ex-info "tern requested its exit" {})))]
          (binding [sqlserver/*plan* (atom [{:create-index :ix_n :on :t :columns [:a]}])]
            (vec (sqlserver/generate-sql (dropping :t :a))))))

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
;; in four) has every one of them checked.
(expect [["i1" "i2" "i3"] ["i1" "i2" "i3"]]
        (lookups-on-three-rows))
