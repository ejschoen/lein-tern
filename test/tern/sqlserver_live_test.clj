(ns tern.sqlserver-live-test
  "The SQL Server index-existence guard, `tern.sqlserver/index-exists?`, run
  against a real SQL Server.

  Runs only when TERN_SQLSERVER_HOST is non-blank, reading TERN_SQLSERVER_PORT
  (default 1433), TERN_SQLSERVER_DATABASE, TERN_SQLSERVER_USER and
  TERN_SQLSERVER_PASSWORD alongside it; otherwise it registers no expectations.
  With the host set, a blank database, user or password stops the run at load
  with a message that names only the blank variable.
  It is read-only: it reads the catalog to find an existing user table and one
  of its named indexes, plus an index on another table that the first table
  lacks, then asks the guard about all three.  Nothing is created, altered or
  dropped."
  (:require [tern.sqlserver]
            [tern.db           :refer [db-spec]]
            [clojure.java.jdbc :as jdbc]
            [clojure.string    :as s]
            [expectations      :refer :all]))

(defn- live-db
  "The connection map for the live server, or nil when TERN_SQLSERVER_HOST is
  blank.  Throws when the host is set and the database or the login is blank;
  the message names the first blank variable and carries no value."
  []
  (let [host (System/getenv "TERN_SQLSERVER_HOST")]
    (when-not (s/blank? host)
      (doseq [v ["TERN_SQLSERVER_DATABASE" "TERN_SQLSERVER_USER" "TERN_SQLSERVER_PASSWORD"]]
        (when (s/blank? (System/getenv v))
          (throw (ex-info (str "tern.sqlserver-live-test: TERN_SQLSERVER_HOST is set but " v " is blank") {}))))
      {:host                   host
       :port                   (or (System/getenv "TERN_SQLSERVER_PORT") "1433")
       :database               (System/getenv "TERN_SQLSERVER_DATABASE")
       :user                   (System/getenv "TERN_SQLSERVER_USER")
       :password               (System/getenv "TERN_SQLSERVER_PASSWORD")
       :subprotocol            "sqlserver"
       :trustServerCertificate true})))

(def ^:private catalog-sql
  ;; The first user table with a named index, and that index; then, from another
  ;; table, the first named index whose name the first table does not have.
  (str "SELECT TOP 1 o.name AS tbl, i.name AS idx, x.other_tbl, x.other_idx"
       " FROM sys.indexes i JOIN sys.objects o ON o.object_id = i.object_id"
       " OUTER APPLY (SELECT TOP 1 o2.name AS other_tbl, i2.name AS other_idx"
       "              FROM sys.indexes i2 JOIN sys.objects o2 ON o2.object_id = i2.object_id"
       "              WHERE o2.type = 'U' AND i2.name IS NOT NULL AND o2.object_id <> o.object_id"
       "                AND i2.name NOT IN (SELECT i3.name FROM sys.indexes i3"
       "                                    WHERE i3.object_id = o.object_id AND i3.name IS NOT NULL)"
       "              ORDER BY o2.name, i2.name) x"
       " WHERE o.type = 'U' AND i.name IS NOT NULL"
       " ORDER BY o.name, i.name"))

(defn- catalog-row
  "Reads the catalog row.  A failed query surfaces with the host and database
  only: the driver's own exception is not chained, since its message can name
  the login."
  [db]
  (let [row (try
              (first (jdbc/query (db-spec db) [catalog-sql]))
              (catch Exception e
                (throw (ex-info (str "tern.sqlserver-live-test: catalog query failed against "
                                     (:host db) "/" (:database db)
                                     " (" (.getName (class e)) ")")
                                {}))))]
    (when-not (:tbl row)
      (throw (ex-info (str "tern.sqlserver-live-test: no user table with a named index in " (:database db)) {})))
    row))

(if-let [db (live-db)]
  (let [{:keys [tbl idx other_tbl other_idx]} (catalog-row db)]
    (println (str "tern.sqlserver-live-test: ran against " (:host db) "/" (:database db)
                  " table " tbl " index " idx))

    ;; An index the catalog says exists must be found by the guard.
    (expect true
            (boolean (#'tern.sqlserver/index-exists? db tbl idx)))

    ;; An index that does not exist, on the same table, must not be.
    (expect false
            (boolean (#'tern.sqlserver/index-exists? db tbl "tern_live_test_no_such_index")))

    ;; An index that exists, but on another table, must not be found on this one.
    (if other_idx
      (do (println (str "tern.sqlserver-live-test: other-table check asks table " tbl
                        " about index " other_idx " (which is on table " other_tbl ")"))
          (expect false
                  (boolean (#'tern.sqlserver/index-exists? db tbl other_idx))))
      (println (str "tern.sqlserver-live-test: skipped the other-table check (no named index on another table"
                    " that table " tbl " lacks, in " (:database db) ")"))))
  (println "tern.sqlserver-live-test: skipped (TERN_SQLSERVER_HOST unset or blank)"))
