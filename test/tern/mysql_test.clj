(ns tern.mysql-test
  (:require [tern.mysql   :refer :all]
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

(expect ["IF NOT EXISTS (SELECT 1 FROM information_schema.TABLE_CONSTRAINTS WHERE CONSTRAINT_SCHEMA=DATABASE() AND CONSTRAINT_NAME='fk_foo_bar' AND CONSTRAINT_TYPE='FOREIGN KEY') THEN ALTER TABLE foo ADD CONSTRAINT fk_foo_bar FOREIGN KEY (bar_id) REFERENCES bar(id)"]
        (generate-sql {:alter-table :foo :add-constraints [[:fk_foo_bar "(bar_id) REFERENCES bar(id)"]]}))

(expect ["IF EXISTS (SELECT 1 FROM information_schema.TABLE_CONSTRAINTS WHERE CONSTRAINT_SCHEMA=DATABASE() AND CONSTRAINT_NAME='fk_foo_bar' AND CONSTRAINT_TYPE='FOREIGN KEY') THEN ALTER TABLE foo DROP FOREIGN KEY fk_foo_bar"]
        (generate-sql {:alter-table :foo :drop-constraints [:fk_foo_bar]}))



