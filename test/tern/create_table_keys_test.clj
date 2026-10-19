(ns tern.create-table-keys-test
  "Foreign keys written inside a :create-table, under :constraints or under
  :add-constraints (the key the :alter-table generator reads, which migrations
  also write inside a create-table), are created by its CREATE TABLE on every
  engine whose generator has generate-constraints: SQL Server, MySQL, H2 v2, H2
  v1 and SQLite (whose helper writes the key without its name).  PostgreSQL's
  create-table has no such helper and writes neither.  Offline: no connection
  is bound, so no guard is asked."
  (:require [tern.sqlserver  :as sqlserver]
            [tern.mysql      :as mysql]
            [tern.h2v2       :as h2v2]
            [tern.h2v1       :as h2v1]
            [tern.sqlite     :as sqlite]
            [tern.postgresql :as postgresql]
            [tern.log]
            [expectations    :refer :all]))

(def ^:private both
  "A create-table step with one key under :constraints and one under :add-constraints."
  {:create-table :t
   :columns [[:id "INT"] [:a_id "INT"] [:b_id "INT"]]
   :primary-key [:id]
   :constraints [[:fk_t_a "(a_id) REFERENCES a(id)"]]
   :add-constraints [[:fk_t_b "(b_id) REFERENCES b(id) ON DELETE CASCADE"]]})

(defn- quietly [generate command]
  (binding [tern.log/*info* false]
    (generate command)))

;; SQL Server: both keys are table constraints of the CREATE TABLE.
(expect ["CREATE TABLE t (CONSTRAINT fk_t_b FOREIGN KEY (b_id) REFERENCES b(id) ON DELETE CASCADE, CONSTRAINT fk_t_a FOREIGN KEY (a_id) REFERENCES a(id), PRIMARY KEY (id), id INT, a_id INT, b_id INT)"]
        (quietly sqlserver/generate-sql both))

;; MySQL.
(expect ["CREATE TABLE t (id INT, a_id INT, b_id INT, PRIMARY KEY (`id`), CONSTRAINT `fk_t_a` FOREIGN KEY (a_id) REFERENCES a(id), CONSTRAINT `fk_t_b` FOREIGN KEY (b_id) REFERENCES b(id) ON DELETE CASCADE)"]
        (quietly mysql/generate-sql both))

;; H2 v2, which upper-cases the names.
(expect ["CREATE TABLE t (CONSTRAINT FK_T_B FOREIGN KEY (b_id) REFERENCES b(id) ON DELETE CASCADE, CONSTRAINT FK_T_A FOREIGN KEY (a_id) REFERENCES a(id), PRIMARY KEY (id), ID INT, A_ID INT, B_ID INT)"]
        (quietly h2v2/generate-sql both))

;; H2 v1.
(expect ["CREATE TABLE t (CONSTRAINT fk_t_b FOREIGN KEY (b_id) REFERENCES b(id) ON DELETE CASCADE, CONSTRAINT fk_t_a FOREIGN KEY (a_id) REFERENCES a(id), PRIMARY KEY (id), id INT, a_id INT, b_id INT)"]
        (quietly h2v1/generate-sql both))

;; SQLite, whose helper writes each key as FOREIGN KEY <spec> without its name.
(expect ["CREATE TABLE t (id INT, a_id INT, b_id INT, PRIMARY KEY (id), FOREIGN KEY (a_id) REFERENCES a(id), FOREIGN KEY (b_id) REFERENCES b(id) ON DELETE CASCADE)"]
        (quietly sqlite/generate-sql both))

;; A step with neither key writes no key on any engine, and PostgreSQL writes none even with both.
(expect {:sqlserver  ["CREATE TABLE t (PRIMARY KEY (id), id INT, a_id INT, b_id INT)"]
         :mysql      ["CREATE TABLE t (id INT, a_id INT, b_id INT, PRIMARY KEY (`id`))"]
         :h2v2       ["CREATE TABLE t (PRIMARY KEY (id), ID INT, A_ID INT, B_ID INT)"]
         :h2v1       ["CREATE TABLE t (PRIMARY KEY (id), id INT, a_id INT, b_id INT)"]
         :sqlite     ["CREATE TABLE t (id INT, a_id INT, b_id INT, PRIMARY KEY (id))"]
         :postgresql ["CREATE TABLE t (id INT, a_id INT, b_id INT, PRIMARY KEY (id))"]
         :postgresql-with-both ["CREATE TABLE t (id INT, a_id INT, b_id INT, PRIMARY KEY (id))"]}
        (let [neither (dissoc both :constraints :add-constraints)]
          {:sqlserver  (quietly sqlserver/generate-sql neither)
           :mysql      (quietly mysql/generate-sql neither)
           :h2v2       (quietly h2v2/generate-sql neither)
           :h2v1       (quietly h2v1/generate-sql neither)
           :sqlite     (quietly sqlite/generate-sql neither)
           :postgresql (quietly postgresql/generate-sql neither)
           :postgresql-with-both (quietly postgresql/generate-sql both)}))

;; With table options, SQL Server builds the table through an :alter-table, which is handed both keys.
(expect ["CREATE TABLE t (__placeholder int)"
         "ALTER TABLE t ADD id INT,a_id INT,b_id INT, CONSTRAINT fk_t_a FOREIGN KEY (a_id) REFERENCES a(id),CONSTRAINT fk_t_b FOREIGN KEY (b_id) REFERENCES b(id) ON DELETE CASCADE"
         "ALTER TABLE t ROW_FORMAT=Compressed"
         "ALTER TABLE t ADD PRIMARY KEY (id)"
         "ALTER TABLE t DROP column __placeholder"]
        (vec (quietly sqlserver/generate-sql (assoc both :table-options [{:name "ROW_FORMAT" :value "Compressed"}]))))

;; So does MySQL.
(expect ["CREATE TABLE t (__placeholder int)"
         "ALTER TABLE `t` ROW_FORMAT=Compressed, ADD COLUMN `id` INT, ADD COLUMN `a_id` INT, ADD COLUMN `b_id` INT, ADD CONSTRAINT `fk_t_a` FOREIGN KEY (a_id) REFERENCES a(id), ADD CONSTRAINT `fk_t_b` FOREIGN KEY (b_id) REFERENCES b(id) ON DELETE CASCADE"
         "ALTER TABLE `t` ADD PRIMARY KEY (`id`)"
         "ALTER TABLE `t` DROP COLUMN `__placeholder`"]
        (vec (quietly mysql/generate-sql (assoc both :table-options [{:name "ROW_FORMAT" :value "Compressed"}]))))

;; A create-table skipped because the table already exists skips its keys with it (SQL Server,
;; the table lookup saying yes for the bound connection and the exact table only).
(expect nil
        (let [db {:subprotocol "sqlserver" :host "sqlserver.invalid" :port "1433" :database "i2kdocs"}]
          (with-redefs [sqlserver/table-exists? (fn [d t] (= [d t] [db "t"]))]
            (binding [sqlserver/*db*   db
                      sqlserver/*plan* (atom [])]
              (quietly sqlserver/generate-sql both)))))
