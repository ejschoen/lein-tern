(ns tern.sqlserver-drop-columns-live-test
  "tern's SQL Server `:drop-columns` against a real SQL Server: a migration whose
  column drop meets an index that still contains the column is refused before any
  of its statements runs, and one that drops those indexes first runs whole; and
  two guards that read the migration's own earlier commands: an index the
  migration creates is dropped by its {:drop-index}, and a column the migration
  adds after dropping and creating its table again is added.

  Runs only when TERN_SQLSERVER_HOST is non-blank and
  TERN_SQLSERVER_SCRATCH_DATABASE names a scratch database,
  i2k_tern_probe_<14 digits>_dc, created beforehand through i2kdb's own create
  path; otherwise it registers no expectations.  With both set, a blank
  TERN_SQLSERVER_DATABASE, TERN_SQLSERVER_USER or TERN_SQLSERVER_PASSWORD stops
  the run at load with a message that names only the blank variable, as in
  `tern.sqlserver-live-test`; so does a scratch name equal to
  TERN_SQLSERVER_DATABASE.

  It writes, in the scratch database only, and only after SELECT DB_NAME()
  confirms that is where it is connected.  It creates a table
  <t> = tern_dc_<UTC yyyyMMddHHmmss> (id INT NOT NULL, a VARCHAR(10) NULL, b INT
  NULL DEFAULT 0) whose column a is in two indexes, <t>_ix_a (a) and <t>_ix_a_id
  (a, id), with a third index, <t>_ix_id (id), and a version table <t>_versions
  made by tern's own create-version-table, and two more tables, <t>_ci and <t>_rc,
  each (id INT NOT NULL, a INT NULL).  Then it runs five migrations through
  tern's SqlServerMigrator, as a migrate does, with tern's exit trapped (a stub
  that records the status and throws, as System/exit never returns) and the lines
  tern logs as errors and the statements it executes recorded:

    1. :drop-columns [:a :b] alone (version 1): refused, one line for each of a's
       two indexes; nothing executed, the catalog as before, no version row
       (lines DROP-COLUMNS-REFUSED and DROP-COLUMNS-UNCHANGED);
    2. {:drop-index <t>_ix_a} then the same :drop-columns (version 2): refused for
       <t>_ix_a_id; nothing executed, so <t>_ix_a is still there, and no version
       row (line DROP-COLUMNS-FIRST-COMMAND);
    3. both :drop-index commands, then the :drop-columns (version 3): the two
       DROP INDEX statements and the column drop with its default, nothing else,
       no IF EXISTS statement; a, b, the default and both indexes gone, id and
       <t>_ix_id kept, the version row written (line DROP-COLUMNS-APPLIED);
    4. a :create-index of <t>_ci_ix_a on <t>_ci (a), then a {:drop-index
       <t>_ci_ix_a :on <t>_ci} (version 4): both statements, the index gone
       afterwards, the version row written (line DROP-INDEX-CREATED);
    5. a :drop-table of <t>_rc, a :create-table of it with id alone, then an
       :alter-table adding a (version 5): the three statements, a there
       afterwards, the version row written (line ADD-COLUMNS-AFTER-DROP-TABLE).

  When every check passed it drops its four tables (tables-dropped);
  otherwise it keeps them for inspection (tables-kept) and prints what it found.
  It never creates or drops a database.  Nothing it prints carries the login or
  the password."
  (:require [tern.sqlserver    :as sqlserver]
            [tern.db           :as tern-db :refer [db-spec]]
            [tern.log          :as log]
            [clojure.java.jdbc :as jdbc]
            [clojure.string    :as s]
            [expectations      :refer :all])
  (:import [java.time ZoneOffset ZonedDateTime]
           [java.time.format DateTimeFormatter]
           [java.util.regex Pattern]))

(def ^:private label "tern.sqlserver-drop-columns-live-test: ")

(def ^:private scratch-pattern
  "The only database names this test writes in."
  #"i2k_tern_probe_\d{14}_dc")

(defn- live-scratch
  "{:db <connection map on the scratch database>} or {:skip <reason>}.  Throws
  when the host and the scratch name are set but the database, the login or the
  password is blank, or the scratch name is the configured database; the message
  names the variable and carries no value."
  []
  (let [host    (System/getenv "TERN_SQLSERVER_HOST")
        scratch (System/getenv "TERN_SQLSERVER_SCRATCH_DATABASE")]
    (cond
      (s/blank? host)
      {:skip "TERN_SQLSERVER_HOST unset or blank"}

      (or (s/blank? scratch) (not (re-matches scratch-pattern scratch)))
      {:skip "TERN_SQLSERVER_SCRATCH_DATABASE unset or not a scratch name"}

      :else
      (do (doseq [v ["TERN_SQLSERVER_DATABASE" "TERN_SQLSERVER_USER" "TERN_SQLSERVER_PASSWORD"]]
            (when (s/blank? (System/getenv v))
              (throw (ex-info (str label "TERN_SQLSERVER_HOST is set but " v " is blank") {}))))
          (when (= scratch (System/getenv "TERN_SQLSERVER_DATABASE"))
            (throw (ex-info (str label "TERN_SQLSERVER_SCRATCH_DATABASE names TERN_SQLSERVER_DATABASE, not a scratch database") {})))
          {:db {:host                   host
                :port                   (or (System/getenv "TERN_SQLSERVER_PORT") "1433")
                :database               scratch
                :user                   (System/getenv "TERN_SQLSERVER_USER")
                :password               (System/getenv "TERN_SQLSERVER_PASSWORD")
                :subprotocol            "sqlserver"
                :trustServerCertificate true}}))))

(defn- scrub
  "`message` with the configured password and login blanked out."
  [db message]
  (let [{:keys [user password]} db]
    (cond-> (str message)
      (not (s/blank? password)) (s/replace password "****")
      (not (s/blank? user))     (s/replace (re-pattern (str "(?<![A-Za-z0-9_])" (Pattern/quote user) "(?![A-Za-z0-9_])"))
                                           "****"))))

(defn- names
  [conn sql & params]
  (set (map :n (jdbc/query conn (into [sql] params)))))

(defn- dependents
  "What the catalog holds for `table`, read without tern's own lookups: whether
  the table exists, which of its columns id, a and b exist, its named indexes,
  its default constraints."
  [conn table]
  {:tables   (names conn "SELECT name AS n FROM sys.tables WHERE name = ?" table)
   :columns  (names conn (str "SELECT column_name AS n FROM information_schema.columns"
                              " WHERE table_name = ? AND column_name IN ('id', 'a', 'b')")
                    table)
   :indexes  (names conn (str "SELECT i.name AS n FROM sys.indexes i JOIN sys.objects o ON o.object_id = i.object_id"
                              " WHERE o.name = ? AND i.name IS NOT NULL")
                    table)
   :defaults (names conn (str "SELECT d.name AS n FROM sys.default_constraints d"
                              " JOIN sys.objects o ON o.object_id = d.parent_object_id WHERE o.name = ?")
                    table)})

(defn- versions
  "The versions recorded in the version table `vt`."
  [conn vt]
  (names conn (str "SELECT version AS n FROM " vt)))

(defn- index-names
  [table]
  {:a    (str table "_ix_a")
   :a-id (str table "_ix_a_id")
   :id   (str table "_ix_id")})

(defn- expected-before
  "The catalog the test builds; the default's name is the server's, so it is counted."
  [table]
  (let [ix (index-names table)]
    {:tables   #{table}
     :columns  #{"id" "a" "b"}
     :indexes  #{(:a ix) (:a-id ix) (:id ix)}
     :defaults 1}))

(defn- before-summary
  [{:keys [before]}]
  (when before
    (update before :defaults count)))

(defn- expected-after-applied
  "What the third migration must leave: the table, id and the index on id."
  [table]
  {:tables   #{table}
   :columns  #{"id"}
   :indexes  #{(:id (index-names table))}
   :defaults #{}})

(defn- refusal
  "The line tern logs when `column` of `table` cannot be dropped because `index`
  still contains it."
  [table column index]
  (str "tern: cannot drop column " table "." column " on SQL Server: index " index " still contains it;"
       " drop the index first in this migration (:drop-index), or remove a stale index by hand"
       " (see the SQL Server migration runbook, section 5a)"))

(defn- applied-statements
  "The statements the third migration's commands must execute, in order, the
  version row's insert aside: the two index drops, then the default and the
  columns in one statement."
  [table default]
  (let [ix (index-names table)]
    [(format "DROP INDEX %s ON %s" (:a ix) table)
     (format "DROP INDEX %s ON %s" (:a-id ix) table)
     (format "ALTER TABLE %s DROP constraint if exists %s, column a,b" table default)]))

(defn- more-tables
  "The tables and the index of migrations 4 and 5."
  [table]
  {:ci    (str table "_ci")
   :ci-ix (str table "_ci_ix_a")
   :rc    (str table "_rc")})

(defn- created-index-statements
  "The statements the fourth migration's commands must execute, in order, the
  version row's insert aside: the create of the index, then its drop."
  [table]
  (let [{:keys [ci ci-ix]} (more-tables table)]
    [(format "CREATE  INDEX %s ON %s (a)" ci-ix ci)
     (format "DROP INDEX %s ON %s" ci-ix ci)]))

(defn- recreated-statements
  "The statements the fifth migration's commands must execute, in order, the
  version row's insert aside: the table dropped, created again with id alone,
  and a added."
  [table]
  (let [{:keys [rc]} (more-tables table)]
    [(format "DROP TABLE %s" rc)
     (format "CREATE TABLE %s (id INT NOT NULL)" rc)
     (format "ALTER TABLE %s ADD a INT NULL" rc)]))

(defn- command-statements
  "What a run executed, the version row's insert into `vt` aside."
  [vt executed]
  (vec (remove #(s/starts-with? % (str "INSERT INTO " vt " ")) executed)))

(defn- migrate!
  "Runs `commands` as `version` through tern's SqlServerMigrator on the scratch
  database, `vt` its version table, with tern's exit trapped and every line tern
  logs as an error (still printed) and every statement it executes (still
  executed) recorded.  Returns {:exit the status of a requested exit, or nil,
  :errors [...], :executed [...]}; anything else tern throws propagates."
  [db vt version commands]
  (let [exit         (atom nil)
        errors       (atom [])
        executed     (atom [])
        real-error   log/error
        real-execute @#'sqlserver/execute-statement!]
    (with-redefs-fn {#'sqlserver/exit!             (fn [status]
                                                     (reset! exit status)
                                                     (throw (ex-info "tern requested its exit" {::exit status})))
                     #'log/error                   (fn [& args]
                                                     (swap! errors conj (s/join " " args))
                                                     (apply real-error args))
                     #'sqlserver/execute-statement! (fn [conn sql]
                                                      (swap! executed conj sql)
                                                      (real-execute conn sql))}
      (fn []
        (try
          (tern-db/migrate (sqlserver/->SqlServerMigrator {:db db :version-table vt}) version commands)
          (catch clojure.lang.ExceptionInfo e
            (when-not (contains? (ex-data e) ::exit)
              (throw e))))))
    {:exit @exit :errors @errors :executed @executed}))

(defn- checks
  "Each check's verdict on `result`."
  [{:keys [table version-table before refused first-command applied created-index recreated]}]
  (let [ix      (index-names table)
        more    (more-tables table)
        default (first (:defaults before))]
    {:before        (= (expected-before table) (when before (update before :defaults count)))
     :refused       (and (= 1 (:exit refused))
                         (= 2 (count (:errors refused)))
                         (= #{(refusal table "a" (:a ix)) (refusal table "a" (:a-id ix))}
                            (set (:errors refused))))
     :unchanged     (and (some? refused)
                         (= [] (:executed refused))
                         (= before (:after refused))
                         (= #{} (:versions refused)))
     :first-command (and (= 1 (:exit first-command))
                         (= [(refusal table "a" (:a-id ix))] (:errors first-command))
                         (= [] (:executed first-command))
                         (= before (:after first-command))
                         (= #{} (:versions first-command)))
     :applied       (and (some? applied)
                         (nil? (:exit applied))
                         (= [] (:errors applied))
                         (= (applied-statements table default) (command-statements version-table (:executed applied)))
                         (not-any? #(s/includes? % "IF EXISTS") (:executed applied))
                         (= (expected-after-applied table) (:after applied))
                         (= #{"3"} (:versions applied)))
     :created-index (and (some? created-index)
                         (nil? (:exit created-index))
                         (= [] (:errors created-index))
                         (= (created-index-statements table) (command-statements version-table (:executed created-index)))
                         (= #{(:ci more)} (:tables (:after created-index)))
                         (= #{} (:indexes (:after created-index)))
                         (contains? (:versions created-index) "4"))
     :recreated     (and (some? recreated)
                         (nil? (:exit recreated))
                         (= [] (:errors recreated))
                         (= (recreated-statements table) (command-statements version-table (:executed recreated)))
                         (= #{"id" "a"} (:columns (:after recreated)))
                         (contains? (:versions recreated) "5"))}))

(defn- passed?
  [result]
  (and (nil? (:error result))
       (every? true? (vals (checks result)))))

(defn- run-drop-columns!
  "Builds the table, its three indexes, the version table and the two tables of
  migrations 4 and 5 in the scratch database, runs the five migrations, reading
  the catalog and the version table after each, and drops the four tables when
  every check passed.
  Never throws: a failure is returned under :error, scrubbed, naming the step."
  [db]
  (let [table  (str "tern_dc_" (.format (DateTimeFormatter/ofPattern "yyyyMMddHHmmss")
                                        (ZonedDateTime/now ZoneOffset/UTC)))
        vt     (str table "_versions")
        ix     (index-names table)
        t      (keyword table)
        drop-a (fn [index] {:drop-index (keyword index) :on t})
        drop-c {:alter-table t :drop-columns [:a :b]}
        {:keys [ci ci-ix rc]} (more-tables table)
        step   (atom "connecting")
        result (atom {:table table :version-table vt})]
    (try
      (jdbc/with-db-connection [conn (db-spec db)]
        (reset! step "checking the connected database")
        (let [current (:d (first (jdbc/query conn ["SELECT DB_NAME() AS d"])))]
          (when-not (= current (:database db))
            (throw (ex-info (str "connected to " current ", not " (:database db) "; nothing created") {}))))
        (reset! step "creating the table")
        (jdbc/execute! conn [(format "CREATE TABLE %s (id INT NOT NULL, a VARCHAR(10) NULL, b INT NULL DEFAULT 0)" table)]
                       :transaction? false)
        (swap! result assoc :created true)
        (doseq [sql [(format "CREATE INDEX %s ON %s (a)" (:a ix) table)
                     (format "CREATE INDEX %s ON %s (a, id)" (:a-id ix) table)
                     (format "CREATE INDEX %s ON %s (id)" (:id ix) table)]]
          (jdbc/execute! conn [sql] :transaction? false))
        (reset! step "creating the version table")
        (#'sqlserver/create-version-table db vt)
        (reset! step "creating the tables of migrations 4 and 5")
        (doseq [more [ci rc]]
          (jdbc/execute! conn [(format "CREATE TABLE %s (id INT NOT NULL, a INT NULL)" more)] :transaction? false))
        (reset! step "reading the catalog before")
        (swap! result assoc :before (dependents conn table))
        (doseq [[k version on commands]
                [[:refused       "1" table [drop-c]]
                 [:first-command "2" table [(drop-a (:a ix)) drop-c]]
                 [:applied       "3" table [(drop-a (:a ix)) (drop-a (:a-id ix)) drop-c]]
                 [:created-index "4" ci    [{:create-index (keyword ci-ix) :on (keyword ci) :columns [:a]}
                                            {:drop-index (keyword ci-ix) :on (keyword ci)}]]
                 [:recreated     "5" rc    [{:drop-table (keyword rc)}
                                            {:create-table (keyword rc) :columns [[:id "INT" "NOT NULL"]]}
                                            {:alter-table (keyword rc) :add-columns [[:a "INT" "NULL"]]}]]]]
          (reset! step (str "running migration " version))
          (let [run (migrate! db vt version commands)]
            (reset! step (str "reading the catalog after migration " version))
            (swap! result assoc k (assoc run
                                         :after    (dependents conn on)
                                         :versions (versions conn vt)))))
        (when (passed? @result)
          (reset! step "dropping the tables")
          (doseq [created [table vt ci rc]]
            (jdbc/execute! conn [(format "DROP TABLE %s" created)] :transaction? false))
          (swap! result assoc
                 :dropped true
                 :left    (names conn "SELECT name AS n FROM sys.tables WHERE name IN (?, ?, ?, ?)" table vt ci rc))))
      (catch Exception e
        (swap! result assoc :error (scrub db (str "at " @step ": " (.getMessage e))))))
    @result))

(defn- after-state
  "present or absent: whether `name` is among the `k` (:indexes or :columns) a
  migration's run found afterwards; unknown when it found nothing."
  [run k name]
  (cond (nil? (:after run))                  "unknown"
        (contains? (k (:after run)) name)    "present"
        :else                                "absent"))

(defn- result-lines
  [{:keys [table version-table applied created-index recreated created dropped] :as result}]
  (let [c    (checks result)
        ix   (index-names table)
        more (more-tables table)]
    [(str "DROP-COLUMNS-REFUSED table=" table " "
          (if (:refused c) (str "exit=1 indexes=" (:a ix) "," (:a-id ix)) "FAILED"))
     (str "DROP-COLUMNS-UNCHANGED table=" table " "
          (if (:unchanged c) "statements-run=0 version-rows=0" "FAILED"))
     (if (:first-command c)
       (str "DROP-COLUMNS-FIRST-COMMAND not-applied version-row absent table=" table " refused-index=" (:a-id ix))
       (str "DROP-COLUMNS-FIRST-COMMAND FAILED table=" table))
     (str "DROP-COLUMNS-APPLIED table=" table " "
          (if (:applied c)
            (str "statements=" (count (command-statements version-table (:executed applied)))
                 " columns-gone default-gone indexes-gone other-index-kept no-if-exists version-row written")
            "FAILED"))
     (str "DROP-INDEX-CREATED table=" (:ci more) " index=" (:ci-ix more)
          " after=" (after-state created-index :indexes (:ci-ix more)) " "
          (if (:created-index c) "statements=2 version-row written" "FAILED"))
     (str "ADD-COLUMNS-AFTER-DROP-TABLE table=" (:rc more) " column=a"
          " after=" (after-state recreated :columns "a") " "
          (if (:recreated c) "statements=3 version-row written" "FAILED")
          " "
          (cond dropped "tables-dropped"
                created "tables-kept"
                :else   "table-not-created"))]))

(let [{:keys [db skip]} (live-scratch)]
  (if db
    (let [result (run-drop-columns! db)
          {:keys [table before refused first-command applied created-index recreated]} result
          ix     (index-names table)]
      (println (str label "ran against " (:host db) "/" (:database db)
                    " table " table " and " (:version-table result)))
      (doseq [line (result-lines result)]
        (println line))
      (when-not (passed? result)
        (println (str label "what it found: " (pr-str (dissoc result :table :version-table)))))

      ;; Every step ran, and the table had what it was built with: a in two indexes, a third
      ;; index on id, a default on b.
      (expect nil
              (:error result))

      (expect (expected-before table)
              (before-summary result))

      ;; 1. The column drop alone is refused for each of a's indexes, by name ...
      (expect {:exit 1 :errors #{(refusal table "a" (:a ix)) (refusal table "a" (:a-id ix))} :count 2}
              {:exit (:exit refused) :errors (set (:errors refused)) :count (count (:errors refused))})

      ;; ... before anything ran: nothing executed, the catalog as it was, no version row.
      (expect {:executed [] :after before :versions #{}}
              (select-keys refused [:executed :after :versions]))

      ;; 2. With one of the two index drops first, the drop is refused for the other, and the
      ;; first command never reached the database: its index is still there, no version row.
      (expect {:exit 1 :errors [(refusal table "a" (:a-id ix))] :executed [] :after before :versions #{}}
              (select-keys first-command [:exit :errors :executed :after :versions]))

      ;; 3. With both index drops first, the migration runs whole: the two DROP INDEX
      ;; statements, then the default and both columns in one statement, nothing else; a, b,
      ;; the default and the two indexes gone, id and its index kept, the version recorded.
      (expect {:exit     nil
               :errors   []
               :executed (applied-statements table (first (:defaults before)))
               :after    (expected-after-applied table)
               :versions #{"3"}}
              (-> (select-keys applied [:exit :errors :executed :after :versions])
                  (update :executed #(command-statements (:version-table result) %))))

      ;; 4. An index created and then dropped in one migration: the drop-index guard reads the
      ;; create, so both statements run and the index is gone; the version recorded.
      (expect {:exit nil :errors [] :executed (created-index-statements table) :indexes #{} :version-row true}
              {:exit        (:exit created-index)
               :errors      (:errors created-index)
               :executed    (command-statements (:version-table result) (:executed created-index))
               :indexes     (:indexes (:after created-index))
               :version-row (contains? (:versions created-index) "4")})

      ;; 5. A table dropped and created again without a, then a added: the add-columns guard reads
      ;; the drop, though the catalog read before the migration shows a, so the three statements
      ;; run and a is there; the version recorded.
      (expect {:exit nil :errors [] :executed (recreated-statements table) :columns #{"id" "a"} :version-row true}
              {:exit        (:exit recreated)
               :errors      (:errors recreated)
               :executed    (command-statements (:version-table result) (:executed recreated))
               :columns     (:columns (:after recreated))
               :version-row (contains? (:versions recreated) "5")})

      ;; Every check passed, so the four tables were dropped and the scratch database keeps none.
      (expect {:dropped true :left #{}}
              (select-keys result [:dropped :left])))
    (println (str label "skipped (" skip ")"))))
