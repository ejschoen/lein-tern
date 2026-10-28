(ns tern.single-drop-test
  "An :alter-table may name one column under :drop-columns, or one constraint
  under :drop-constraints, as a keyword rather than a vector of them, as the SQL
  Server generator already reads it.  The MySQL, H2 v2, H2 v1 and SQLite
  generators threw on that form (they iterated over the keyword); they now read
  it as the one-element vector, in the command's own keys and where they read
  an earlier command of the migration's plan.  The plan readers are checked with
  the catalog lookups replaced by stubs that answer yes only for the bound
  connection and the exact names."
  (:require [tern.mysql  :as mysql]
            [tern.h2v2   :as h2v2]
            [tern.h2v1   :as h2v1]
            [tern.sqlite :as sqlite]
            [tern.log]
            [expectations :refer :all]))

(defn- single-and-vector
  "What `generate` gives for one constraint and one column written as keywords,
  then as one-element vectors."
  [generate]
  (binding [tern.log/*info* false]
    [[(generate {:alter-table :t :drop-constraints :fk_x}) (generate {:alter-table :t :drop-columns :c})]
     [(generate {:alter-table :t :drop-constraints [:fk_x]}) (generate {:alter-table :t :drop-columns [:c]})]]))

(defn- same-both-ways
  [[single vector]]
  (and (= single vector) single))

;; MySQL.
(expect [["ALTER TABLE `t` DROP FOREIGN KEY `fk_x`"] ["ALTER TABLE `t` DROP COLUMN `c`"]]
        (same-both-ways (single-and-vector mysql/generate-sql)))

;; H2 v2.
(expect [["ALTER TABLE T DROP CONSTRAINT IF EXISTS FK_X"] ["alter table T drop column C"]]
        (same-both-ways (single-and-vector h2v2/generate-sql)))

;; H2 v1.
(expect [["ALTER TABLE t DROP FOREIGN KEY fk_x"] ["alter table t drop column c"]]
        (same-both-ways (single-and-vector h2v1/generate-sql)))

;; SQLite.
(expect [["ALTER TABLE t DROP FOREIGN KEY fk_x"] ["ALTER TABLE t DROP COLUMN c"]]
        (same-both-ways (single-and-vector sqlite/generate-sql)))

(def ^:private db {:subprotocol "test" :subname "//catalog.invalid/unused"})

;; MySQL's plan reader: a column and a constraint the catalog shows are added again when an earlier
;; command of the migration dropped them, each named by a single keyword.
(expect ["ALTER TABLE `t` ADD COLUMN `c` INT, ADD CONSTRAINT `fk_x` FOREIGN KEY (`c`) REFERENCES `u`(`id`)"]
        (with-redefs [mysql/column-exists?      (fn [d t c] (= [d t c] [db "`t`" "`c`"]))
                      mysql/foreign-key-exists? (fn [d fk] (= [d fk] [db "`fk_x`"]))]
          (binding [tern.log/*info* false
                    mysql/*db*      db
                    mysql/*plan*    (atom [{:alter-table :t :drop-columns :c :drop-constraints :fk_x}])]
            (mysql/generate-sql {:alter-table :t :add-columns [[:c "INT"]]
                                 :add-constraints [[:fk_x "(c) REFERENCES u(id)"]]}))))

;; H2 v2's, H2 v1's and SQLite's plan readers: a column the catalog shows is added again after an
;; earlier single-keyword drop of it, and on H2 a primary key the catalog shows is added again after
;; an earlier single-keyword drop of :primary-key.
(expect {:h2v2   ["alter table T add column (C INT)" "ALTER TABLE T ADD PRIMARY KEY (id)"]
         :h2v1   ["alter table t add column (c INT)" "ALTER TABLE T ADD PRIMARY KEY (id)"]
         :sqlite ["ALTER TABLE t ADD COLUMN c INT"]}
        (binding [tern.log/*info* false]
          {:h2v2 (with-redefs [h2v2/column-exists?      (fn [d t c] (= [d t c] [db "T" "c"]))
                               h2v2/primary-key-exists? (fn [d t] (= [d t] [db "T"]))]
                   (binding [h2v2/*db*   db
                             h2v2/*plan* (atom [{:alter-table :t :drop-columns :c :drop-constraints :primary-key}])]
                     (vec (h2v2/generate-sql {:alter-table :t :add-columns [[:c "INT"]] :primary-key [:id]}))))
           :h2v1 (with-redefs [h2v1/column-exists?      (fn [d t c] (= [d t c] [db "t" "c"]))
                               h2v1/primary-key-exists? (fn [d t] (= [d t] [db "t"]))]
                   (binding [h2v1/*db*   db
                             h2v1/*plan* (atom [{:alter-table :t :drop-columns :c :drop-constraints :primary-key}])]
                     (vec (h2v1/generate-sql {:alter-table :t :add-columns [[:c "INT"]] :primary-key [:id]}))))
           :sqlite (with-redefs [sqlite/column-exists? (fn [d t c] (= [d t c] [db "t" "c"]))]
                     (binding [sqlite/*db*   db
                               sqlite/*plan* (atom [{:alter-table :t :drop-columns :c}])]
                       (vec (sqlite/generate-sql {:alter-table :t :add-columns [[:c "INT"]]}))))}))
