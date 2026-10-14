(ns tern.sqlserver-guard-test
  "The SQL Server index-existence guard without a server.

  The guard's query is checked by replacing `clojure.java.jdbc/query` with a
  stub that captures the parameter vector it is handed.  The two index
  generators are checked with the guard itself replaced and `*db*` bound, so
  the guard is consulted (with `*db*` nil, as in `tern.sqlserver-test`, the
  generators never ask it)."
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
  (with-redefs [sqlserver/index-exists? (fn [_ _ _] answer)]
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
