(ns tern.sqlserver-live-test
  "The SQL Server index-existence guard, `tern.sqlserver/index-exists?`, run
  against a real SQL Server.

  Runs only when TERN_SQLSERVER_HOST is set, reading TERN_SQLSERVER_PORT
  (default 1433), TERN_SQLSERVER_DATABASE, TERN_SQLSERVER_USER and
  TERN_SQLSERVER_PASSWORD alongside it; otherwise it registers no expectations.
  It is read-only: it reads the catalog to find an existing user table and one
  of its named indexes, then asks the guard about that index and about one that
  does not exist.  Nothing is created, altered or dropped."
  (:require [tern.sqlserver]
            [tern.db           :refer [db-spec]]
            [clojure.java.jdbc :as jdbc]
            [expectations      :refer :all]))

(if-let [host (System/getenv "TERN_SQLSERVER_HOST")]
  (let [db {:host                   host
            :port                   (or (System/getenv "TERN_SQLSERVER_PORT") "1433")
            :database               (System/getenv "TERN_SQLSERVER_DATABASE")
            :user                   (System/getenv "TERN_SQLSERVER_USER")
            :password               (System/getenv "TERN_SQLSERVER_PASSWORD")
            :subprotocol            "sqlserver"
            :trustServerCertificate true}
        {:keys [tbl idx]}
        (first (jdbc/query (db-spec db)
                           [(str "SELECT TOP 1 o.name AS tbl, i.name AS idx"
                                 " FROM sys.indexes i JOIN sys.objects o ON o.object_id = i.object_id"
                                 " WHERE o.type = 'U' AND i.name IS NOT NULL"
                                 " ORDER BY o.name, i.name")]))]
    (println (str "tern.sqlserver-live-test: ran against " host "/" (:database db)
                  " table " tbl " index " idx))

    ;; An index the catalog says exists must be found by the guard.
    (expect true
            (boolean (#'tern.sqlserver/index-exists? db tbl idx)))

    ;; An index that does not exist, on the same table, must not be.
    (expect false
            (boolean (#'tern.sqlserver/index-exists? db tbl "tern_live_test_no_such_index"))))
  (println "tern.sqlserver-live-test: skipped (TERN_SQLSERVER_HOST unset)"))
