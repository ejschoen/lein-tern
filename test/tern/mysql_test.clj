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


