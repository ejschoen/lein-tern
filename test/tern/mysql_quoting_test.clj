(ns tern.mysql-quoting-test
  "The MySQL generator backticks every identifier it emits, so a reserved word
  (GROUPS is reserved from MySQL 8.0.2, RANK from 8.0.2) works as a table or
  column name: the table and column names of a CREATE TABLE, the table of a DROP
  TABLE, and the referencing columns, the referenced table and the referenced
  columns of a foreign-key spec `(<cols>) REFERENCES <table>(<cols>) <rest>`,
  written inside a create-table or added by an alter-table.  A name already in
  backticks is kept, a dotted `db.table` is quoted per part, the rest of the
  spec (ON DELETE ...) and its blanks and case are kept, and a spec of another
  shape is emitted as written.  Offline: no connection is bound."
  (:require [tern.mysql   :as mysql]
            [tern.log]
            [expectations :refer :all]))

(defn- quietly [command]
  (binding [tern.log/*info* false]
    (vec (mysql/generate-sql command))))

(defn- add-key [table key-name spec]
  (quietly {:alter-table table :add-constraints [[key-name spec]]}))

;; A create-table of groups, with a column rank, a primary key and a key on groups itself.
(expect ["CREATE TABLE `groups` (`id` INT NOT NULL, `rank` INT, `groups_id` INT, PRIMARY KEY (`id`), CONSTRAINT `fk_groups_parent` FOREIGN KEY (`groups_id`) REFERENCES `groups`(`id`) ON DELETE CASCADE)"]
        (quietly {:create-table :groups
                  :columns [[:id "INT" "NOT NULL"] [:rank "INT"] [:groups_id "INT"]]
                  :primary-key [:id]
                  :constraints [[:fk_groups_parent "(groups_id) REFERENCES groups(id) ON DELETE CASCADE"]]}))

;; A spec already in backticks is unchanged.
(expect ["ALTER TABLE `group_users` ADD CONSTRAINT `fk_gu_groups` FOREIGN KEY (`groups_id`) REFERENCES `groups`(`id`)"]
        (add-key :group_users :fk_gu_groups "(`groups_id`) REFERENCES `groups`(`id`)"))

;; A dotted referenced table is quoted per part.
(expect ["ALTER TABLE `t` ADD CONSTRAINT `fk_t_groups` FOREIGN KEY (`groups_id`) REFERENCES `other`.`groups`(`id`)"]
        (add-key :t :fk_t_groups "(groups_id) REFERENCES other.groups(id)"))

;; A spec of another shape (here without the referenced columns) is emitted as written.
(expect ["ALTER TABLE `t` ADD CONSTRAINT `fk_t_x` FOREIGN KEY (groups_id) REFERENCES groups"]
        (add-key :t :fk_t_x "(groups_id) REFERENCES groups"))

;; A drop-table of groups.
(expect ["DROP TABLE `groups`"]
        (quietly {:drop-table :groups}))

;; With table options the create-table goes through a placeholder table and an alter-table.
(expect ["CREATE TABLE `groups` (`__placeholder` int)"
         "ALTER TABLE `groups` ROW_FORMAT=Compressed, ADD COLUMN `id` INT, ADD COLUMN `rank` INT, ADD CONSTRAINT `fk_g` FOREIGN KEY (`id`) REFERENCES `groups`(`id`)"
         "ALTER TABLE `groups` ADD PRIMARY KEY (`id`)"
         "ALTER TABLE `groups` DROP COLUMN `__placeholder`"]
        (quietly {:create-table :groups
                  :columns [[:id "INT"] [:rank "INT"]]
                  :primary-key [:id]
                  :table-options [{:name "ROW_FORMAT" :value "Compressed"}]
                  :constraints [[:fk_g "(id) REFERENCES groups(id)"]]}))

;; An alter-table adding a key that references groups.
(expect ["ALTER TABLE `group_users` ADD CONSTRAINT `fk_gu_groups` FOREIGN KEY (`groups_id`) REFERENCES `groups`(`id`) ON DELETE CASCADE"]
        (add-key :group_users :fk_gu_groups "(groups_id) REFERENCES groups(id) ON DELETE CASCADE"))

;; A composite key keeps the spec's blanks and case.
(expect ["ALTER TABLE `t` ADD CONSTRAINT `fk_t_ab` FOREIGN KEY ( `a_id` , `b_id` ) references `Groups` ( `id` , `x` ) on delete set null"]
        (add-key :t :fk_t_ab "( a_id , b_id ) references Groups ( id , x ) on delete set null"))
