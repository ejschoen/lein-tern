(ns tern.sqlserver-test
  (:require [tern.sqlserver   :refer :all]
            [expectations :refer :all]))

(expect ["CREATE TABLE foo (a INT)"]
        (generate-sql {:create-table :foo :columns [[:a "INT"]]}))

(expect ["CREATE TABLE foo (a VARCHAR(7) CHECK (a IN('Hello','Goodbye')))"]
        (generate-sql {:create-table :foo :columns [[:a "ENUM('Hello','Goodbye')"]]}))

(expect ["CREATE TABLE foo (PRIMARY KEY (a), a INT"]
        (generate-sql {:create-table :foo :columns [[:a "INT"]] :primary-key [:a]}))

(expect ["CREATE TABLE foo (CONSTRAINT fk_a FOREIGN KEY (a) REFERENCES foo(a), a INT)"]
        (generate-sql {:create-table :foo :columns [[:a "INT"]] :constraints [[:fk_a "(a) REFERENCES foo(a)"]]}))

(expect ["CREATE TABLE foo (CONSTRAINT fk_a FOREIGN KEY (a) REFERENCES foo(a), PRIMARY KEY (a), a INT)"]
        (generate-sql {:create-table :foo :columns [[:a "INT"]] :primary-key [:a] :constraints [[:fk_a "(a) REFERENCES foo(a)"]]}))

(expect ["INSERT INTO foo VALUES (1,2,\"foo\"),(3,4,\"bar\")"]
        (generate-sql {:insert-into :foo :values [[1 2 "foo"] [3 4 "bar"]]}))

(expect '("ALTER TABLE foo ADD CONSTRAINT fk_foo_bar FOREIGN KEY (bar_id) REFERENCES bar(id)")
        (generate-sql {:alter-table :foo :add-constraints [[:fk_foo_bar "(bar_id) REFERENCES bar(id)"]]}))

(expect '("ALTER TABLE foo ADD CONSTRAINT fk_foo_bar FOREIGN KEY (bar_id) REFERENCES bar(id)"
          "ALTER TABLE foo ROW_FORMAT=Compressed")
        (generate-sql {:alter-table :foo
                       :table-options [{:name "ROW_FORMAT" :value "Compressed"}]
                       :add-constraints [[:fk_foo_bar "(bar_id) REFERENCES bar(id)"]]}))

(expect '("CREATE TABLE foo (__placeholder int)"
          "ALTER TABLE foo ADD a INT,b INT"
          "ALTER TABLE foo ROW_FORMAT=Compressed"
          "ALTER TABLE foo ADD PRIMARY KEY (a)"
          "ALTER TABLE foo DROP column __placeholder")
        (generate-sql {:create-table :foo
                       :primary-key [:a]
                       :table-options [{:name "ROW_FORMAT" :value "Compressed"}]
                       :columns [[:a "INT"] [:b "INT"]]}))

