(ns tern.sqlserver-drop-columns-live-test
  "tern's SQL Server `:drop-columns` against a real SQL Server: the indexes and
  the default constraint that depend on a column are dropped before the column.

  Runs only when TERN_SQLSERVER_HOST is non-blank and
  TERN_SQLSERVER_SCRATCH_DATABASE names a scratch database,
  i2k_tern_probe_<14 digits>_dc, created beforehand through i2kdb's own create
  path; otherwise it registers no expectations.  With both set, a blank
  TERN_SQLSERVER_DATABASE, TERN_SQLSERVER_USER or TERN_SQLSERVER_PASSWORD stops
  the run at load with a message that names only the blank variable, as in
  `tern.sqlserver-live-test`; so does a scratch name equal to
  TERN_SQLSERVER_DATABASE.

  It writes, in the scratch database only, and only after SELECT DB_NAME()
  confirms that is where it is connected: it creates a table
  tern_dc_<UTC yyyyMMddHHmmss> (id INT NOT NULL, a VARCHAR(10) NULL, b INT NULL
  DEFAULT 0) with two indexes on a, (a) and (a, id), and one on (id, b), checks
  that those four dependents exist, binds `*db*` to the scratch database and
  `*plan*` to an empty plan, runs `generate-sql` for `:drop-columns [:a :b]`,
  executes every statement in order, and reads the catalog: a, b, the three
  indexes and the default must be gone, the table and its column id still
  there.  It prints one DROP-COLUMNS-LIVE line.  A second table, the same with
  the suffix _r, runs the same way except that between tern generating its
  statements and their execution the test re-creates _a2_idx on (id) alone:
  tern's statement for that index must then leave it, and the line is
  DROP-COLUMNS-LIVE-REDEFINED.  A table is kept (the line ends table-kept)
  unless TERN_SCRATCH_DROP_TABLE is true and every check on it passed; then the
  test drops that table (table-dropped).  It never
  creates or drops a database.  Nothing it prints carries the login or the
  password."
  (:require [tern.sqlserver    :as sqlserver]
            [tern.db           :refer [db-spec]]
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

(defn- expected-before
  [table]
  {:tables   #{table}
   :columns  #{"id" "a" "b"}
   :indexes  #{(str table "_a_idx") (str table "_a2_idx") (str table "_id_b_idx")}
   :defaults 1})

(defn- expected-after
  "What must be left: the table and id; no index, no default.  In the redefined
  scenario _a2_idx, re-created on (id) alone after tern generated its statements,
  must survive with that definition."
  [table redefined?]
  (cond-> {:tables   #{table}
           :columns  #{"id"}
           :indexes  #{}
           :defaults #{}}
    redefined? (assoc :indexes #{(str table "_a2_idx")}
                      :a2-columns #{"id"})))

(defn- index-columns
  [conn table index]
  (names conn (str "SELECT c.name AS n FROM sys.indexes i"
                   " JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id"
                   " JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id"
                   " JOIN sys.objects o ON o.object_id = i.object_id WHERE o.name = ? AND i.name = ?")
         table index))

(defn- before-summary
  [{:keys [before]}]
  (when before
    (update before :defaults count)))

(defn- passed?
  [{:keys [table redefined? error after] :as result}]
  (and (nil? error)
       (= (expected-before table) (before-summary result))
       (= (expected-after table redefined?) after)))

(defn- run-drop-columns!
  "Creates the table with its four dependents in the scratch database, has tern
  drop the two columns, executes its statements in order, and reads the catalog.
  With `redefined?`, between tern generating its statements and their execution
  it drops _a2_idx and re-creates it on (id) alone, as a migration could, so the
  run-time condition of tern's statement for that index must leave it alone.
  Never throws: a failure is returned under :error, scrubbed, naming the step."
  [db redefined?]
  (let [table  (str "tern_dc_" (.format (DateTimeFormatter/ofPattern "yyyyMMddHHmmss")
                                        (ZonedDateTime/now ZoneOffset/UTC))
                    (when redefined? "_r"))
        step   (atom "connecting")
        result (atom {:table table :redefined? redefined? :statements 0})]
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
        (doseq [sql [(format "CREATE INDEX %s_a_idx ON %s (a)" table table)
                     (format "CREATE INDEX %s_a2_idx ON %s (a, id)" table table)
                     (format "CREATE INDEX %s_id_b_idx ON %s (id, b)" table table)]]
          (jdbc/execute! conn [sql] :transaction? false))
        (reset! step "reading the catalog before")
        (swap! result assoc :before (dependents conn table))
        (reset! step "generating the statements")
        (let [statements (vec (binding [sqlserver/*db*   db
                                        sqlserver/*plan* (atom [])]
                                (doall (sqlserver/generate-sql {:alter-table (keyword table)
                                                                :drop-columns [:a :b]}))))]
          (swap! result assoc :statements (count statements))
          (when redefined?
            (reset! step "re-creating _a2_idx on (id)")
            (doseq [sql [(format "DROP INDEX %s_a2_idx ON %s" table table)
                         (format "CREATE INDEX %s_a2_idx ON %s (id)" table table)]]
              (println (str label "before tern's statements: " sql))
              (jdbc/execute! conn [sql] :transaction? false)))
          (doseq [[i sql] (map-indexed vector statements)]
            (println (str label "statement " (inc i) " of " (count statements) ": " sql))
            (reset! step (str "statement " (inc i) " of " (count statements)))
            (jdbc/execute! conn [sql] :transaction? false)))
        (reset! step "reading the catalog after")
        (swap! result assoc :after (cond-> (dependents conn table)
                                     redefined? (assoc :a2-columns (index-columns conn table (str table "_a2_idx")))))
        (when (and (passed? @result) (= "true" (System/getenv "TERN_SCRATCH_DROP_TABLE")))
          (reset! step "dropping the table")
          (jdbc/execute! conn [(format "DROP TABLE %s" table)] :transaction? false)
          (swap! result assoc :dropped true)))
      (catch Exception e
        (swap! result assoc :error (scrub db (str "at " @step ": " (.getMessage e))))))
    @result))

(defn- result-line
  [{:keys [table redefined? statements created dropped error before after] :as result}]
  (str (if redefined? "DROP-COLUMNS-LIVE-REDEFINED" "DROP-COLUMNS-LIVE")
       " table=" table " statements=" statements " "
       (if (passed? result)
         (if redefined?
           "columns-gone redefined-index-kept other-indexes-gone default-gone"
           "columns-gone indexes-gone default-gone")
         (s/join " " (remove nil?
                             ["FAILED"
                              (when (and before (not= (expected-before table) (before-summary result)))
                                (str "dependents-before=" (pr-str before)))
                              (when (and after (not= (expected-after table redefined?) after))
                                (str "after=" (pr-str after)))
                              (when error (str "error=" (pr-str error)))])))
       " "
       (cond dropped     "table-dropped"
             created     "table-kept"
             :else       "table-not-created")))

(let [{:keys [db skip]} (live-scratch)]
  (if db
    (let [result    (run-drop-columns! db false)
          redefined (run-drop-columns! db true)]
      (println (str label "ran against " (:host db) "/" (:database db)
                    " table " (:table result) " and " (:table redefined)))
      (println (result-line result))
      (println (result-line redefined))

      ;; The table had what makes a column drop fail on SQL Server: three indexes (two of
      ;; them on a) and a default.
      (expect (expected-before (:table result))
              (before-summary result))

      ;; tern's statements ran without error, in order (a dependent left behind is error 5074).
      (expect nil
              (:error result))

      ;; a and b, the three indexes and the default are gone; the table and id are not.
      (expect #{"id"}
              (:columns (:after result)))

      (expect #{}
              (:indexes (:after result)))

      (expect #{}
              (:defaults (:after result)))

      (expect #{(:table result)}
              (:tables (:after result)))

      ;; The redefined scenario: the same dependents before, no error, and afterwards a and
      ;; b, _a_idx, _id_b_idx and the default gone while _a2_idx, re-created on (id) between
      ;; generation and execution, survives with that definition.
      (expect (expected-before (:table redefined))
              (before-summary redefined))

      (expect nil
              (:error redefined))

      (expect (expected-after (:table redefined) true)
              (:after redefined)))
    (println (str label "skipped (" skip ")"))))
