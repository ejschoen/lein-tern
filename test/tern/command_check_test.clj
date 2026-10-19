(ns tern.command-check-test
  "tern refuses a migration leg holding a command it would misread, before any
  of its SQL is generated: a key no generator reads for the command (a misspelt
  :modify_columns was ignored without a word), a command with no command key,
  and a command with two command keys (tern runs the first alone).  The :drop-index
  of a step whose command is :alter-table is that step's own key.  run checks the
  :up leg and rollback the :down leg, each only the leg about to run.  tern's
  exit (tern.migrate/exit!) is replaced by a stub that records the status and
  throws, as System/exit never returns, and tern.log/error is captured."
  (:require [tern.migrate      :as migrate]
            [tern.db           :refer [Migrator]]
            [tern.log]
            [clojure.string    :as s]
            [clojure.java.io   :as io]
            [expectations      :refer :all])
  (:import [java.nio.file Files Path Paths]
           [java.nio.file.attribute FileAttribute]))

(defn- problems
  "command-problems of `commands`, each as [problem keys]."
  [& commands]
  (mapv (juxt :problem :keys) (migrate/command-problems "20990101000000-x.edn" (vec commands))))

;; A key no generator reads for the command, in the command's own key order.
(expect [[:unknown-keys [:modify_columns]]]
        (problems {:alter-table :t :modify_columns [[:c "INT"]]}))

;; A command with no command key; it has no offending key of its own.
(expect [[:no-command-key []]]
        (problems {:drop-constraints [:fk_x]}))

;; A command with two command keys, both named in their written order.
(expect [[:two-command-keys [:drop-table :update]]]
        (problems (array-map :drop-table :t :update "SELECT 1")))

;; An :alter-table step that lists its own indexes under :drop-index, as seven old steps do, is
;; accepted: its command is :alter-table, the key it is written with first.
(expect []
        (problems (array-map :alter-table :t :drop-constraints [:fk] :drop-columns [:c] :drop-index [:ix])))

;; Written before :alter-table, :drop-index is the key tern runs, so the step holds two command keys.
(expect [[:two-command-keys [:drop-index :alter-table]]]
        (problems (array-map :drop-index [:ix] :alter-table :t :drop-columns [:c])))

;; Every key some generator reads for each command, with every dialect override, is accepted.
(expect []
        (problems {:create-table :t :columns [] :primary-key [:id] :constraints [] :add-constraints []
                   :table-options [] :sqlserver "x"}
                  {:drop-table :t :mysql "x" :h2 "x"}
                  {:alter-table :t :add-columns [] :drop-columns [] :modify-columns [] :modify-column [:c "INT"]}
                  {:alter-table :t :alter-columns [] :add-constraints [] :drop-constraints [] :drop-index []}
                  {:alter-table :t :primary-key [:id] :table-options [] :character-set {} :postgresql "x" :sqlite "x"}
                  {:create-index :i :on :t :columns [:c] :unique true}
                  {:drop-index :i :on :t}
                  {:insert-into :t :values [] :columns [] :query "x"}
                  {:update "x" :sqlserver "x" :mysql "x" :h2 "x"}))

;; A leg element that is not a map is a problem; a leg written as one map is read as one command,
;; and a missing leg has none.
(expect [[[:not-a-map []]] [[:unknown-keys [:delete-columns]]] []]
        [(problems :alter-table)
         (mapv (juxt :problem :keys) (migrate/command-problems "f.edn" {:alter-table :t :delete-columns [:c]}))
         (migrate/command-problems "f.edn" nil)])

(defn- checked
  "check-commands! on `commands`, its exit trapped and its error lines captured:
  {:errors [...] :exit <status or nil> :returned <its value, when it returns>}."
  [leg commands]
  (let [errors (atom [])]
    (with-redefs [tern.log/error (fn [& parts] (swap! errors conj (s/join " " parts)))
                  migrate/exit!  (fn [status] (throw (ex-info "tern requested its exit" {::exit status})))]
      (let [outcome (try {:returned (migrate/check-commands! "20990101000000-x.edn" leg commands)}
                         (catch clojure.lang.ExceptionInfo e {:exit (::exit (ex-data e))}))]
        (assoc outcome :errors @errors)))))

;; A leg with problems: one line per problem naming the file, the leg, the command whole and the
;; offending keys, a closing line, and the exit with status 1.
(expect {:exit 1
         :errors ["tern: 20990101000000-x.edn, down command 1, {:alter-table :t, :delete-columns [:c]}: has keys no generator reads for :alter-table: (:delete-columns)"
                  "tern: 20990101000000-x.edn, down command 2, {:drop-constraints [:fk_x]}: has no command key (one of :create-table :drop-table :alter-table :create-index :drop-index :insert-into :update), keys ()"
                  "tern: 20990101000000-x.edn was not run: fix the commands of its down leg above; no statement of it has run"]}
        (checked :down [{:alter-table :t :delete-columns [:c]} {:drop-constraints [:fk_x]}]))

;; A leg without a problem logs nothing, does not exit, and returns nil.
(expect {:returned nil :errors []}
        (checked :up [{:alter-table :t :modify-columns [[:c "INT"]]} {:drop-table :t}]))

;; --- run and rollback check the leg about to run, and only that leg ------------

(defrecord RecordingMigrator [calls]
  Migrator
  (init     [this] nil)
  (version  [this] nil)
  (versions [this] nil)
  (migrate  [this version commands]
    (swap! calls conj [:up version]))
  (migrate  [this from version commands]
    (swap! calls conj [:down from version])))

(def ^:private scratch-dir
  (delay (doto (.toFile (Files/createTempDirectory "tern-command-check" (make-array FileAttribute 0)))
           (.deleteOnExit))))

(defn- migration-file
  "A migration file named `name` holding `migration`, as a Path."
  [name migration]
  (let [f (io/file @scratch-dir name)]
    (spit f (pr-str migration))
    (.deleteOnExit f)
    (.toPath f)))

(defn- outcome
  "What `f` does with a recording migrator, tern's exit trapped: {:calls [...] :exit <status or nil>}."
  [f]
  (let [calls (atom [])]
    (with-redefs [tern.log/error (fn [& _] nil)
                  migrate/exit!  (fn [status] (throw (ex-info "tern requested its exit" {::exit status})))]
      (let [exit (try (f (->RecordingMigrator calls)) nil
                      (catch clojure.lang.ExceptionInfo e (::exit (ex-data e))))]
        {:calls @calls :exit exit}))))

(def ^:private bad-up
  (delay (migration-file "20990101000001-bad-up.edn"
                         {:up [{:alter-table :t :modify_columns [[:c "INT"]]}] :down [{:alter-table :t :drop-columns [:c]}]})))

(def ^:private bad-down
  (delay (migration-file "20990101000002-bad-down.edn"
                         {:up [{:alter-table :t :add-columns [[:c "INT"]]}] :down [{:alter-table :t :delete-columns [:c]}]})))

;; run refuses a bad :up before the migrator is called; rollback refuses a bad :down the same way.
(expect [{:calls [] :exit 1} {:calls [] :exit 1}]
        [(outcome #(migrate/run % @bad-up))
         (outcome #(migrate/rollback % @bad-down "20990101000001"))])

;; Each checks only its own leg: run applies a migration whose :down is bad, and rollback runs a
;; migration whose :up is bad.
(expect [{:calls [[:up "20990101000002"]] :exit nil} {:calls [[:down "20990101000001" "0"]] :exit nil}]
        [(outcome #(migrate/run % @bad-down))
         (outcome #(migrate/rollback % @bad-up "0"))])

;; tern's own example migrations pass on both legs.
(expect []
        (vec (for [dir ["examples/postgres-project/migrations" "examples/mysql-project/migrations"]
                   f (sort (.listFiles (io/file dir)))
                   :let [m (read-string (slurp f))]
                   leg [:up :down]
                   problem (migrate/command-problems (.getName f) (get m leg))]
               problem)))
