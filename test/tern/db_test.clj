(ns tern.db-test
  (:require [tern.db      :refer :all]
            [expectations :refer :all]))

;; concatenates db config
(expect "//localhost:5432/animals"
        (subname {:host "localhost"
                  :port 5432
                  :database "animals"}))

;; `db-spec` assocs the subname into the db configuration
(expect {:host "localhost"
         :port 5432
         :database "animals"
         :subname "//localhost:5432/animals"}
        (db-spec {:host "localhost"
                  :port 5432
                  :database "animals"}))

;; `:database` can be overridden
(expect {:host "localhost"
         :port 5432
         :database "pets"
         :subname "//localhost:5432/pets"}
        (db-spec {:host "localhost"
                  :port 5432
                  :database "animals"} "pets"))

;; snake-cases and stringifies
(expect "favourite_foods"
        (to-sql-name :favourite-foods))

;; --- dialect override normalization -------------------------------------------
;;
;; An override is hand-written, so it may be a bare string, a vector of strings,
;; or -- in the oldest migrations -- a vector wrapping a nested vector.  All
;; three have to arrive at the executor as a flat sequence of statements.
;;
;; The nested form used to be swallowed whole.  `jdbc/db-do-commands` has
;; arglist `[db transaction? & commands]`, so a vector argument bound
;; `transaction?` and left `commands` empty; `executeBatch` then ran against a
;; statement with nothing added to it, reported success, and the version row was
;; written anyway.  Such migrations were skipped silently for years.

(expect ["SELECT 1"]
        (sql-statements "SELECT 1"))

(expect ["SELECT 1" "SELECT 2"]
        (sql-statements ["SELECT 1" "SELECT 2"]))

(expect []
        (sql-statements nil))

;; The shape from 20160423191540-add-groups.edn.  Order has to survive too:
;; `SET IDENTITY_INSERT` is session-scoped and only covers the insert after it.
(expect ["SET IDENTITY_INSERT DBO.GROUPS ON"
         "insert into groups(id,clients_id,groupname) values (1,1,'i2k')"
         "SET IDENTITY_INSERT DBO.GROUPS OFF"]
        (sql-statements [["SET IDENTITY_INSERT DBO.GROUPS ON"
                          "insert into groups(id,clients_id,groupname) values (1,1,'i2k')"
                          "SET IDENTITY_INSERT DBO.GROUPS OFF"]]))

;; Arbitrary depth, since nothing guarantees only one level of nesting.
(expect ["a" "b" "c" "d"]
        (sql-statements ["a" [["b" "c"] "d"]]))

;; A non-string leaf is rejected loudly rather than silently dropped.
(expect clojure.lang.ExceptionInfo
        (sql-statements [42]))

;; ...and the rejection names the migration and the offending command.
(expect #"Migration 20160423191540.*42.*:create-table"
        (try (sql-statements [42] {:version "20160423191540"
                                   :command {:create-table :groups}})
             (catch clojure.lang.ExceptionInfo e (.getMessage e))))
