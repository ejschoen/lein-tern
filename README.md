# tern - migrations as data

[![Build Status](https://travis-ci.org/bugsbio/lein-tern.svg?branch=master)](https://travis-ci.org/bugsbio/lein-tern)

The [Artic Tern](http://en.wikipedia.org/wiki/Arctic_tern) holds the
long-distance record for bird migration; therein lies the flimsy justification
for the name of this plugin, which handles database migrations.

![Arctic Tern by OddurBen](images/tern.jpg)
<figcaption>
  Arctic tern 8664 <a href="http://creativecommons.org/licenses/by-sa/3.0">CC BY-SA 3.0</a>
  by <a href="http://commons.wikimedia.org/wiki/User:OddurBen">OddurBen</a>
</figcaption>

## Motivation

There are already a number of good migration plugins for Clojure, such as [Drift](https://github.com/macourtney/drift),
which abstract the concept of versioned changes away from any specific datastore.
You can use whatever datastore you like, and whatever library you want to run the individual migrations. Drift has no opinion about it.

This is great - but it is a tradeoff. You get this flexibility in exchange for having to deal
with the specifics of your datastore yourself - in every migration that you write. We didn't want to do have to do this, so we created Tern. Tern lets you specify your migrations as data, like this:

```clojure
{:up [{:create-table :cats
       :columns [:name "VARCHAR(56)" "NOT NULL DEFAULT 'MOGGY'"]}
      {:alter-table :dogs
       :add-columns [[:dislikes "NOT NULL DEFAULT 'cats'"]]}]
 :down [{:drop-table :cats}
        {:alter-table :dogs
         :drop-columns [:dislikes]]}
```

The trade-off here is that if you want to do something in a migration, Tern has to support it. The upside is that the code to support it only needs to be written once, and we've put some effort into making Tern easy to extend with new commands and new datastores. For example, while Tern was originally written with Postgres support only, [@eightbitraptor](https://github.com/eightbitraptor/) was able to add MySQL support over the course of a few hours at the London Clojure Dojo.

## Usage

NB. There are example postgresql and mysql projects in the `examples` directory.

### Installation & Configuration

Add `[cc-artifice/lein-tern "0.5.1"]` to the `:plugins` vector of your project.clj, and add a `:tern` key with the following (optional) configuration, shown with default values:

```clojure
(defproject postgres-project "0.1.0"
  :description "Example Postgresql project using Tern"
  :url "http://github.com/bugsbio/lein-tern"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :plugins [[cc-artifice/lein-tern "0.5.1"]]
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [postgresql "9.3-1102.jdbc41"]]

  :profiles {:dev {:source-paths ["dev"]}}

  :tern {:migration-dir "migrations"
         :version-table "schema_versions"
         :color true
         :db {:host        "localhost"
              :port        5432
              :database    "postgres"
              :user        "postgres"
              :password    ""
              :subprotocol "postgresql"}})
```

You almost certainly won't want to have your DB config in your `project.clj` like this - for a start you'll already have configured it elsewhere, and it will be different depending on which environment you're running in. So Tern allows you to specify an extra configuration key `:init`, which should be a namespace-qualified symbol identifying a function on your classpath that returns a config map. This map will be merged into the map specified in your `project.clj`.

Again, see the example projects for details, but here's what it might look like, assuming you keep your config in environment variables, [12 Factor App](http://12factor.net/) style:

```clojure
;; project.clj
(defproject postgres-project "0.1.0"
  ;; ...
  :profiles {:dev {:source-paths ["dev"]}}
  :tern {:init postgres-project.migrations/configure
         :migration-dir "migrations"})
         
;; dev/postgres-project/migrations.clj
(ns postgres-project.migrations)
(defn configure []
  {:db {:classname   "org.postgresql.Driver"
        :subprotocol "postgresql"
        :host        (System/getenv "DB_HOST")
        :database    (System/getenv "DB_DATABASE")
        :user        (System/getenv "DB_USER")
        :password    (System/getenv "DB_PASSWORD")}})
```

You can now see a list of available commands by running `lein help tern`, and confirm that Tern is using the correct configuration values by running `lein tern config`.

You can now either create the database, user, and version table specified in your config yourself, or run `lein tern init` to let Tern create them using the default postgres or mysql user, if no password is set. (This is probably best used only in development.)

### Creating migrations

Create a new migration with `lein tern new-migration [name]`.

![Creating a migration](images/screenshot-create-migration.png)

This will create a migration file with empty `:up` and `:down` vectors in the migrations directory. Edit the file and add the commands you wish to perform:

```clojure
{:up
 [{:create-table :cats
   :columns [[:id   "BIGSERIAL" "PRIMARY KEY"]
             [:name "TEXT"      "NOT NULL"]
             [:paws "INT"]]}]
 :down
 [{:drop-table :cats}]}
```

Currently Tern supports creating and dropping tables, columns, and indexes, for postgres and mysql, using the following syntax. (The column specs match those supported by [clojure.java.jdbc](https://github.com/clojure/java.jdbc/), which is what Tern uses under the hood):

```clojure
;; Creating a table
{:create-table :cats :columns [[:name "VARCHAR(255)" "NOT NULL DEFAULT 'Kitty'"]]}

;; Creating a table with a composite primary key
{:create-table :cat-ownerns
 :columns [[:cat_id    "BIGINT" "NOT NULL"]
           [:person_id "BIGINT" "NOT NULL"]]
 ;; MySQL only
 :table-options [{:name :row_format :value "Compressed"}]	   
 :primary-key [:cat_id :person_id]}

;; Creating a table with foreign keys, under :constraints or :add-constraints
{:create-table :cat-owners
 :columns [[:cat_id    "BIGINT" "NOT NULL"]
           [:person_id "BIGINT" "NOT NULL"]]
 :constraints [[:fk_cat_owners_cat "(cat_id) REFERENCES cats(id)"]]
 :add-constraints [[:fk_cat_owners_person "(person_id) REFERENCES person(id) ON DELETE CASCADE"]]}

;; Dropping a table
{:drop-table :cats}

;; Adding columns
{:alter-table :cats :add-columns [[:favourite-food "TEXT" "DEFAULT 'fish'"]]}

;; Dropping columns
{:alter-table :cats
 :drop-columns [:favourite-food]
 :add-columns [[:color "TEXT" "NOT NULL"]]
 :modify-columns [[:favourite-toy "TEXT" "NULL"]]
 :add-constraints [{:fk_person_id_person "(person_id) REFERENCES person(id) ON DELETE NO ACTION"}]
 :drop-constraints [:fk_cat_id_cat]
 :table-options [{:name :row_format :value "Compressed"}]
 ;; MySQL only
 :character-set {:charset-name "utf8" :collation "utf8_general_ci"}}}

;; Creating indexes
{:create-index :cat-name :on :cats :unique true :columns [:name]}

;; Dropping indexes
{:drop-index :cat-name :on :cats}

;; Inserting seed values (constants, or by querying another table)  -- MySQL only
{:insert-into :cats
 :values [[1 "Fluffy" 4]
          [2 "Kitt Peak" 4]]}
{:insert-into :cats
 :query "select * from temporary_cats"}

;; Updating data in place -- MySQL, and SQL Server through its :update method
{:update "update cats set paws=4"}
```

You can have as many commands as you like in the `:up` and `:down` vectors; they will be executed in order.

Foreign keys inside a `:create-table` may be written under `:constraints` or `:add-constraints`, each as `[name spec]`, and the `CREATE TABLE` creates both on SQL Server, MySQL, H2 and SQLite (SQLite writes them without their names; the PostgreSQL generator writes no foreign keys). Before 0.10.20 a key written under `:add-constraints` inside a `:create-table` was ignored.

Each command is a map holding one command key (`:create-table`, `:drop-table`, `:alter-table`, `:create-index`, `:drop-index`, `:insert-into` or `:update`), the keys a generator reads for that command, and any dialect override (`:sqlserver`, `:mysql`, `:h2`, `:postgresql`, `:sqlite`). Before any SQL of a migration is generated, tern checks the leg about to run, the `:up` leg when migrating and the `:down` leg when rolling back: a key no generator reads for the command (a misspelling such as `:modify_columns`), a command with no command key, or a command with two command keys (tern would run only the first, in the order the keys are written) stops the run. tern logs one line per such command, naming the migration file, the command and the offending keys, and exits with status 1 before any statement of the migration runs. The `:drop-index` key of an `:alter-table` written first is that step's own list of indexes, not a second command.

### Running migrations

Run all pending migrations with `lein tern migrate` (or just `lein migrate` if you're lazy).

![Running migrations](images/screenshot-run-migrations.png)

Roll back the last run migration with `lein tern rollback`.

![Rolling back a migration](images/screenshot-rollback.png)

Roll back ALL migrations, ending at the initial state of the database with `lein tern reset`.

![Resetting all migrations](images/screenshot-reset.png) 

## Extending Tern

An in-depth guide to extending Tern to work with more datastores is in the works, but for now, take a look at the `Migrator` protocol defined in [src/tern/db.clj](https://github.com/bugsbio/lein-tern/blob/master/src/tern/db.clj). If you create a type that implements this protocol for your datastore, and register it with Tern by calling `(tern.implementations/register :yourdatastoresubprotocol ->YourMigrator)` (where `->YourMigrator` is a constructor function that takes a single argument, the Tern config map), in the configuration function you defined in your `project.clj`, you should be good to go.

## What's next?

Apart from supporting more commands and more datastores, the fact that Tern migrations are just Clojure maps leads to some interesting possibilities for exploration.
What if you slurp your project's entire history of migrations, and merge them? Can they be merged in such a way as to eliminate changes that were later reverted or replaced, leaving you with a single, simple script to create a fresh database for your project in the shape it is now?

## SQL Server notes

Before emitting `CREATE INDEX` or `DROP INDEX`, the SQL Server generator asks whether the index exists with `OBJECT_ID('<database>..<table>')`; the empty schema part is deliberate, because a two-part `<database>.<table>` is read as `<schema>.<table>`, resolves to NULL and makes every index look absent, while `<database>..<table>` names the database and leaves the schema to the database user's default schema (`dbo` unless the user was created with another). The guard is consulted only when tern holds a connection; with no connection the generators emit the statement unguarded. When the guard does not find the index, `DROP INDEX` is still emitted if an earlier command in the same migration creates the index on that table (a `:create-index` its own guard emits, or a `CREATE INDEX` in an override or an `:update`, read as the column-drop check below reads creates) and no command between drops it again or drops the table, so an index a migration creates and then drops is gone afterwards; a `DROP INDEX` of an index that neither the catalog nor such a create holds is skipped. When only such a create holds the index, the drop is emitted as `DROP INDEX IF EXISTS <index> ON <table>`, so a create read that does not run, or a drop between in a form not read as one (inside the creating command itself, behind a condition, in a statement that holds more than the drop), makes it do nothing rather than fail with error 3701; what this guard can still get wrong on the plan branch is silent, a create written in a form not read (dynamic SQL that does not spell the statement, say), whose index then stays on the table. When the guard finds the index but an earlier command in the same migration may drop it or its table (read as the create-index and add-constraints guards below read drops: a key, or a drop clause anywhere in a statement of an override or a plain `:update`, behind a condition or inside a comment included), the catalog may be stale and the drop is emitted as `DROP INDEX IF EXISTS <index> ON <table>`, which removes the index when it is there and otherwise does nothing, where a plain `DROP INDEX` would fail with error 3701 after a drop that ran; a drop counted that did not run costs nothing here. When the guard finds the index, `CREATE INDEX` is still emitted if an earlier command in the same migration drops it: a `{:drop-index <index> :on <table>}` command, the `:drop-index` key of an `:alter-table` of that table, or a `{:drop-table <table>}`, which drops every index and constraint on the table. The table and index are compared as catalog names (snake_case and unbracketed), so `:my-idx`, `:my_idx` and `"my_idx"` are one index, while a `:drop-index` without `:on` names no table and does not count. Names are compared without regard to case, as SQL Server compares identifiers under its default collation, which is case-insensitive, and as `OBJECT_ID` and `indexproperty` in the guard do, so `:IX_A` drops the index the catalog holds as `ix_a`. On a database whose collation is case-sensitive, spell every index, table and constraint name as the catalog holds it: there a name spelled in another case counts as dropped though the server does not find it, so the statement that relies on the drop (a column drop below, a create, an added constraint) fails when it runs, and two indexes whose names differ only in case count as one, so the refusal below names only one of them. A `DROP INDEX [IF EXISTS] <index> ON <table>[, <index> ON <table>...]` or legacy `DROP INDEX <table>.<index>[, ...]` statement in an earlier command's `:sqlserver` override or plain `:update` counts as dropping those indexes too, a `DROP TABLE [IF EXISTS] <table>[, <table>...]` statement as dropping those tables, and an `ALTER TABLE <table> DROP <item>[, <item>...]` statement as dropping the constraints and columns its whole list names, read item by item as T-SQL reads it: a name at the start of the list or after `CONSTRAINT` is a constraint, every name after `COLUMN` up to the next keyword is a column, and `PERIOD FOR SYSTEM_TIME` is neither, each name with an optional `IF EXISTS` before it and `WITH (...)` after it, so `DROP COLUMN a, CONSTRAINT fk_x` drops the column a and the constraint fk_x, and `DROP CONSTRAINT fk_a WITH (ONLINE = ON), fk_b` both constraints. For these guards such a drop counts wherever it stands in a statement of the override, behind an `IF EXISTS (...)` or an `IF OBJECT_ID(...) IS NOT NULL` included, since a drop counted too often here only makes a create or an add fail when it runs, while the column-drop check below counts only a statement that is the drop alone. Comments, `/* ... */` (which nest) and `-- ...` to the end of the line, are blanked before a statement is read: the column-drop check below reads a statement that is the drop alone either as written or without its comments, so `DROP /* old */ INDEX ix ON t` and `DROP INDEX ix ON t -- gone` count as drops there and a drop written only inside a comment does not, while these guards read the text both as written and without its comments, so for them a comment inside a drop no longer hides it and a drop written only inside a comment still counts; string literals and delimited names are not parsed, so a `--` or `/*` inside one is blanked like a comment, while the text as written, also read, keeps a name such as `[ix--a]` whole. For these guards a command with an override also counts by those of its own `:drop-index`, `:drop-table`, `:drop-constraints` and `:drop-columns` keys whose object the override names somewhere in its text, a key whose object the override never mentions being an instruction for another database: so the guards credit every drop the override writes in the forms above and a drop written another way only when such a key names it, and a drop such a key claims that the override does not perform makes the create or add fail when it runs. A plain `:update`, a command whose command key is `:update`, is read as an override is, since its statements, each string of a vector one statement, are what runs: these guards count a drop anywhere in them, behind a condition or inside a comment included, and the column-drop check below counts only a statement that is the drop alone; such a command has no other keys to count. A command holds one command key, and the readers follow it: a command with two command keys, an `:update` beside another command key among them, stops the run before any of its SQL is generated, whatever order its keys are written or read in (see the command check above), so no such command reaches these readers. A command with both an `:update` and a `:sqlserver` override runs the override alone, and is read by the override alone. `:add-constraints` follows the same rule: it adds a foreign key the catalog still shows when an earlier `:drop-constraints` of it on that table, or a `:drop-table` of the table, dropped it. `:add-columns` reads the migration the same way: it skips adding a column the catalog already shows unless an earlier command, or the `:drop-columns` of the same command (whose drop tern emits before the add), drops the column or its table, by the `:drop-columns` key of an `:alter-table` of the table, a `{:drop-table <table>}`, or the `COLUMN` items of an `ALTER TABLE ... DROP` statement or a `DROP TABLE` statement in an override or a plain `:update`, counted as these guards count drops and compared as catalog names without regard to case; so a column dropped and added again in one command, or added after its table is dropped and created again without it, is added, and a drop counted that did not happen makes the add fail when it runs.

`test/tern/sqlserver_live_test.clj` runs that check against a real SQL Server, configured by five environment variables:

| Variable | Meaning |
|---|---|
| `TERN_SQLSERVER_HOST` | server host; when unset or blank the live test is skipped and registers nothing |
| `TERN_SQLSERVER_PORT` | server port; 1433 when the variable is unset (a blank value is not defaulted) |
| `TERN_SQLSERVER_DATABASE` | database to read |
| `TERN_SQLSERVER_USER` | login |
| `TERN_SQLSERVER_PASSWORD` | password |

With the host set, a blank database, user or password stops the run at load with a message naming only that variable. The live test is read-only: it reads the catalog for an existing table and its indexes and asks the guard about them; it creates, alters and drops nothing.

SQL Server will not drop a column that an index or a default constraint depends on (error 5074, "The index ... is dependent on column ..."), so a `:drop-columns` that runs on MySQL could fail on SQL Server. Every index whose key or included columns contain a column the command drops (found in `sys.indexes` when the migration's SQL is generated) must be dropped earlier in the same migration: by a `{:drop-index <index> :on <table>}` command, by the `:drop-index` key of an `:alter-table` of the table (this command's own included), or with the whole table by a `{:drop-table <table>}`, the names compared as catalog names and without regard to case, as above. The `:drop-index` key of an `:alter-table` names a single index as a keyword or a string (`:drop-index :my-idx`), or several in a vector. Its `:drop-constraints` key likewise names a single constraint as a keyword or a string (`:drop-constraints :fk-x`), the primary key of the table as `:primary-key`, or several in a vector. Its `:drop-columns` key likewise names a single column as a keyword or a string (`:drop-columns :my-col`), or several in a vector. A `DROP INDEX` or `DROP TABLE` statement in an earlier command's `:sqlserver` override or plain `:update`, in the forms above, counts as dropping those indexes or that whole table too. Unlike the guards above, this check counts a command with a `:sqlserver` override only by its statements that are such a drop alone, never by a drop behind a condition and never by its own keys, since the override runs in place of them: a drop not read leaves the refusal in place, which is the safe side here. A plain `:update` counts the same way, by its statements that are such a drop alone, so one that drops the index only behind a condition or inside a comment leaves the refusal in place. An index the migration itself creates on the table before this command counts too, since it is on the table when the column drop runs though the catalog does not show it yet, and must be dropped again after the create: a `:create-index` whose `:columns` include a dropped column (a prefix length such as `"name(10)"` left aside), or a `CREATE [UNIQUE] [CLUSTERED|NONCLUSTERED] [COLUMNSTORE] INDEX <index> ON <table> (<columns>) [INCLUDE (<columns>)]` found anywhere in a statement of an override or of an `:update` whose key or included columns include it, behind an `IF NOT EXISTS`, before a `WHERE`, `WITH` or `ON <filegroup>`, even in a comment or a string, since a create counted too often only refuses a column drop. Such an index is dropped again by a `{:drop-index <index> :on <table>}` command, whose guard emits the drop for an index an earlier command creates (above), by the `:drop-index` key of an `:alter-table` (this command's own included), by a `DROP INDEX` statement in an override or a plain `:update`, or with the whole table. A key column written with a trailing ASC or DESC (`"a DESC"`) counts as that column, and a `:create-index` that its own guard skips, because the catalog already holds an index of that name and no earlier command drops it or its table, creates nothing and does not count. Order is read command by command, not statement by statement, so an override that creates an index and then drops it again still counts as creating it, and the column drop is refused though it would run. Otherwise tern stops while generating the migration's SQL, before any of its statements runs, so nothing of the migration is applied and no version is recorded. It logs one line per column and index, `tern: cannot drop column <table>.<column> on SQL Server: index <index> still contains it; drop the index first in this migration (:drop-index), or remove a stale index by hand (see the SQL Server migration runbook, section 5a)`: add the drop to the migration, or remove an index the database should no longer have as section 5a of that runbook describes. A composite index, a UNIQUE one included, has to be dropped whole and, if still wanted, created again on the remaining columns, where MySQL instead removes the column from the index and keeps it on the rest. The column's default constraint (found in `sys.default_constraints`, which `information_schema` does not list) is dropped in the same `ALTER TABLE` statement as the column, `DROP constraint if exists <default>, column <columns>`, which SQL Server applies whole or not at all, so a column drop that still fails keeps its default; check, primary-key, unique and foreign-key constraints on the column are dropped in the statement before, as before (an index that backs a primary key or a unique constraint is dropped as that constraint, and is not one of the indexes above). Those constraints are looked up in `information_schema.constraint_column_usage` by the table's and the column's catalog names, unbracketed, so a reserved-word table or column (`user`, `public`, bracketed in statements) has its constraints found and dropped. The order is the command's own `:drop-index` key, the drop of those other constraints, then the defaults with the columns.

`test/tern/sqlserver_drop_columns_live_test.clj` checks this against a real server and, unlike the guard test, writes. Besides the variables above it needs `TERN_SQLSERVER_SCRATCH_DATABASE`, the name of an existing scratch database of the form `i2k_tern_probe_<14 digits>_dc` (unset, blank or any other name skips it and registers nothing). In that database it creates a table `tern_dc_<UTC yyyyMMddHHmmss>` whose column `a` is in two indexes, with a third index on `id` and a default on `b`, a version table beside it, and three more tables, `<table>_ci`, `<table>_rc` and `<table>_ud`, then runs six migrations through tern's own migrator with tern's exit trapped. `:drop-columns [:a :b]` alone must be refused naming both of `a`'s indexes, with nothing executed and no version recorded; the same after a `:drop-index` of one of them must be refused for the other, that first drop never executed; with both `:drop-index` commands first the migration must run whole, dropping the two indexes, `a`, `b` and the default and nothing else, and record its version. A fourth creates an index on `<table>_ci` and drops it again, which must leave the index gone, a fifth drops `<table>_rc`, creates it again without its column `a` and adds `a`, which must leave `a` there, and a sixth drops the column `a` of `<table>_ud` by a plain `:update` and adds it again, which must leave `a` there too; each must record its version. When every check passed it drops its five tables; otherwise it keeps them for inspection. It never creates or drops a database.

## Contributing

Pull requests are welcome! Thank you for wanting to contribute. Here's the process we follow:

1. Fork the project, and clone your fork.
2. Create a feature branch `git co -b my-cool-feature`
3. Hack away, making lots of small commits
4. When you're done, rebase against master `git rebase master -i`. This makes sure you're up to date, so your changes will merge cleanly, and lets you squash your commits into one or two high-level ones. Bonus points if your commit messages start with an :sparkles::heart_eyes:[emoji](http://www.emoji-cheat-sheet.com/):heart_eyes::sparkles: that sums up the emotional content of the commit!
5. Create a pull request into our repo. Actually, it's never too early to do this, so if you want help or advice on your change, just go ahead and create a pull request as a place to discuss it as you work on it.

If you're new to open source, or git, or even Clojure, and you're stuck with any of this, don't worry, just create an issue or a pull-request and we'll happily help you out if we can.

## Acknowledgements

Thanks to [@eightbitraptor](https://github.com/eightbitraptor/) for adding MySQL support.

## Changelog
* 0.1.2 Support composite primary keys
* 0.1.3 Breaking change for MySQL users - use Unix timestamp in schema versions table
* 0.10.20 Foreign keys inside a `:create-table` are created whether written under `:constraints` or `:add-constraints` (SQL Server, MySQL, H2, SQLite)
* 0.10.20 A migration leg holding a key no generator reads, a command without a command key or a command with two command keys stops the run before any statement (the leg about to run only)
* 0.10.20 SQL Server: a `DROP INDEX` of an index an earlier command of the migration dropped is emitted as `DROP INDEX IF EXISTS`
* 0.10.20 MySQL, H2 and SQLite: a single keyword under `:drop-columns` or `:drop-constraints` names one column or constraint, as on SQL Server
