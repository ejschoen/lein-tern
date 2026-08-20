(ns tern.mysql-test
  (:require [tern.mysql   :refer :all]
            [tern.log]
            [expectations :refer :all]))

(expect ["CREATE TABLE foo (a INT)"]
        (generate-sql {:create-table :foo :columns [[:a "INT"]]}))

(expect ["CREATE TABLE foo (a INT, PRIMARY KEY (a))"]
        (generate-sql {:create-table :foo :columns [[:a "INT"]] :primary-key [:a]}))

(expect ["CREATE TABLE foo (a INT, CONSTRAINT fk_a FOREIGN KEY (a) REFERENCES foo(a))"]
        (generate-sql {:create-table :foo :columns [[:a "INT"]] :constraints [[:fk_a "(a) REFERENCES foo(a)"]]}))

(expect ["CREATE TABLE foo (a INT, PRIMARY KEY (a), CONSTRAINT fk_a FOREIGN KEY (a) REFERENCES foo(a))"]
        (generate-sql {:create-table :foo :columns [[:a "INT"]] :primary-key [:a] :constraints [[:fk_a "(a) REFERENCES foo(a)"]]}))

(expect ["INSERT INTO foo VALUES (1,2,\"foo\"),(3,4,\"bar\")"]
        (generate-sql {:insert-into :foo :values [[1 2 "foo"] [3 4 "bar"]]}))

(expect ["ALTER TABLE foo ADD CONSTRAINT fk_foo_bar FOREIGN KEY (bar_id) REFERENCES bar(id)"]
        (generate-sql {:alter-table :foo :add-constraints [[:fk_foo_bar "(bar_id) REFERENCES bar(id)"]]}))

(expect ["ALTER TABLE foo ROW_FORMAT=Compressed, ADD CONSTRAINT fk_foo_bar FOREIGN KEY (bar_id) REFERENCES bar(id)"]
        (generate-sql {:alter-table :foo
                       :table-options [{:name "ROW_FORMAT" :value "Compressed"}]
                       :add-constraints [[:fk_foo_bar "(bar_id) REFERENCES bar(id)"]]}))

(expect ["CREATE TABLE foo (__placeholder int)"
         "ALTER TABLE foo ROW_FORMAT=Compressed, ADD COLUMN a INT, ADD COLUMN b INT"
         "ALTER TABLE foo ADD PRIMARY KEY (a)"
         "ALTER TABLE foo DROP COLUMN __placeholder"]
        (generate-sql {:create-table :foo
                       :primary-key [:a]
                       :table-options [{:name "ROW_FORMAT" :value "Compressed"}]
                       :columns [[:a "INT"] [:b "INT"]]}))


;; --- SQL-generation failures abort the migration ------------------------------
;;
;; The catch around `generate-sql` used to log the exception and return nil.
;; That left `sql` nil, so the command contributed no SQL to the run -- and yet
;; the run continued to completion and wrote the version row, recording the
;; migration as applied when it had never executed.  It must propagate instead.
;;
;; `generate-sql` is redefined rather than fed a bad command, because the
;; `:default` method calls `System/exit` and would take the test JVM with it.

(def ^:private run-migration! #'tern.mysql/run-migration!)

(expect RuntimeException
        (with-redefs [tern.mysql/generate-sql
                      (fn [_] (throw (RuntimeException. "generation failed")))
                      ;; the handler logs the failure and its whole stack before
                      ;; rethrowing; silence it so the suite output stays readable
                      tern.log/error (fn [& _] nil)]
          (run-migration! {:db {:subprotocol "mysql" :subname "//localhost/unused"}
                           :version-table "schema_versions"}
                          "20260820000000"
                          [{:create-table :foo}])))

;; A malformed `:mysql` override is rejected before any statement runs, so the
;; migration aborts instead of being recorded as applied.  Normalization itself
;; lives in `tern.db/sql-statements` and is exercised in `tern.db-test`.
(expect clojure.lang.ExceptionInfo
        (run-migration! {:db {:subprotocol "mysql" :subname "//localhost/unused"}
                         :version-table "schema_versions"}
                        "20260820000000"
                        [{:mysql [42]}]))
