(ns tern.migrate
  (:require [tern.db         :as db]
            [tern.file       :refer :all]
            [tern.log        :as log]
            [tern.misc       :refer [last-but-one]]
            [clojure.edn     :as edn]
            [clojure.java.io :as io]
            [clojure.string  :as s]
            [clojure.set     :as set])
  (:import [java.nio.file FileSystems FileSystem Path Paths Files FileVisitOption OpenOption]
           [java.nio.file.spi FileSystemProvider]
           [java.net URI URL URLEncoder]
           [java.io File]
           [java.util Collections]
           [java.util.stream Stream]))

(declare get-migration)

(defmulti enumerate-files (fn [root] (type root)))

(defmethod enumerate-files java.io.File [root]
  (map #(.toPath %) (file-seq root)))
    
(defmethod enumerate-files java.net.URI [root]
  (log/info "Enumerating migration files from" root)
  (let [^Path path  (case (.getScheme root)
                      "jar" (let [^FileSystems fs (or (try (FileSystems/getFileSystem ^URI root)
                                                           (catch java.nio.file.FileSystemNotFoundException _
                                                             nil))
                                                      (try (FileSystems/newFileSystem ^URI root (Collections/emptyMap))
                                                           (catch java.nio.file.FileSystemAlreadyExistsException _
                                                             (FileSystems/getFileSystem ^URI root))))
                                  scheme-specific (.getSchemeSpecificPart root)
                                  scheme-path (.getPath (URI. (s/replace scheme-specific #" " "%20")))
                                  resource-path (second (re-matches #".+!(.+)" scheme-path))]
                              (.getPath fs resource-path (make-array String 0)))
                      "file" (Paths/get root)
                      (Paths/get root))
        ^Stream walk (Files/walk path (into-array FileVisitOption [FileVisitOption/FOLLOW_LINKS]))]
    (log/info "Will walk this path:" path)
    (iterator-seq (.iterator walk))
    ))

(defmethod enumerate-files java.net.URL [root]
  (if root
    (enumerate-files (.toURI root))
    []))


(defn- get-migrations
  "Returns a sequence of all migration files, sorted by name."
  [{:keys [migration-dir]}]
  (log/info "get-migrations called with migration-dir " migration-dir)
  (->> (enumerate-files (or (io/resource migration-dir)
                            (when-let [r (.getResource (type get-migration) migration-dir)] 
                              (try (URI. (s/replace (str r) #" " "%20"))
                                    (catch Exception e
                                      (log/error (format "Unable to find migration resource in %s" migration-dir))
                                      nil)))
                            (io/as-file migration-dir)))
       (filter edn?)
       (sort-by fname)))

(defn parse-version
  [migration]
  (s/replace (basename migration) #"-.*$" ""))

(defn version
  "Returns the most recent migration version."
  [config]
  (when-let [migrations (seq (get-migrations config))]
    (parse-version (last migrations))))

(defn already-run?
  "Returns an anonymous function that checks if a migration is older
  than the given version. Is inclusive of the version equal to its argument."
  [current]
  (fn [migration] (<= (compare (parse-version migration) current) 0)))

(defn pending
  "Returns migrations that need to be run."
  [config current]
  (drop-while (already-run? current) (get-migrations config)))

(defn completed
  "Returns migrations that have already been run."
  [config current]
  (take-while (already-run? current) (get-migrations config)))

(defn missing
  "Return migrations that are missing from the schema-versions table"
  [impl config]
  (let [migrations (get-migrations config)
        version-map (reduce (fn [m migration]
                              (assoc m (parse-version migration) migration))
                            {}
                            migrations)
        db-versions (db/versions impl)]
    (map version-map (sort (set/difference (set (keys version-map)) (set db-versions))))))
  

(defn previous-version
  "Takes a migration version as its argument and returns
  the one immediately preceding it."
  [config current]
  (if-let [previous (last-but-one (completed config current))]
    (parse-version previous)
    "0"))

(defn get-migration
  "Returns the migration for a given version."
  [config version]
  (last (completed config version)))

(defn- path->stream
  [^Path p]
  (.newInputStream ^FileSystemProvider (.provider ^FileSystem (.getFileSystem p))
                   p
                   (make-array OpenOption 0)))

(def command-keys
  "The keys that name a command: the commands every engine's generate-sql
  dispatches on.  A command runs as the first of them in its own key order (a
  migration file read as EDN keeps the written order of a map of up to eight
  keys)."
  [:create-table :drop-table :alter-table :create-index :drop-index :insert-into :update])

(def override-keys
  "The keys that hold one engine's own statements for a command."
  #{:sqlserver :mysql :h2 :postgresql :sqlite})

(def command-argument-keys
  "Per command key, the other keys some engine's generator reads for that
  command (besides override-keys).  The :drop-index of an :alter-table is that
  step's own list of indexes, which the SQL Server generator drops; :alter-columns
  is the PostgreSQL generator's; :modify-column is the one column the SQL Server
  generator alters for each of :modify-columns."
  {:create-table #{:columns :primary-key :constraints :add-constraints :table-options}
   :drop-table   #{}
   :alter-table  #{:add-columns :drop-columns :modify-columns :modify-column :alter-columns
                   :add-constraints :drop-constraints :drop-index :primary-key :table-options
                   :character-set}
   :create-index #{:on :columns :unique}
   :drop-index   #{:on}
   :insert-into  #{:values :columns :query}
   :update       #{}})

(defn- command-problem
  "What is wrong with the `index`th command of a leg, as a map, or nil: a value
  that is not a map; a map with no command key; a map with two command keys
  (tern would run the first alone, the :drop-index of a step whose command is
  :alter-table being that step's own key); or a key that no generator reads for
  its command.  At most one problem per command, in that order; :keys holds the
  offending keys in the command's own key order."
  [file index command]
  (if-not (map? command)
    {:file file :index index :command command :problem :not-a-map :keys []}
    (let [written (vec (keys command))
          named   (filterv (set command-keys) written)
          command-key (first named)
          others  (if (= :alter-table command-key) (filterv #(not= :drop-index %) named) named)]
      (cond
        (nil? command-key)
        {:file file :index index :command command :problem :no-command-key :keys []}

        (< 1 (count others))
        {:file file :index index :command command :problem :two-command-keys :keys others}

        :else
        (let [known (into (conj override-keys command-key) (command-argument-keys command-key))
              unknown (filterv #(not (contains? known %)) written)]
          (when (seq unknown)
            {:file file :index index :command command :problem :unknown-keys :keys unknown
             :command-key command-key}))))))

(defn command-problems
  "The problems of the commands of one leg of the migration `file` (its name),
  each a map naming the file, the command's position and the command itself,
  the kind of problem (:not-a-map, :no-command-key, :two-command-keys or
  :unknown-keys) and the offending keys in the command's own key order (none
  for a command without a command key).  `commands` is the leg as read: a
  vector of command maps, one map, or nil (no problem).  Pure: it reads nothing
  and runs nothing."
  [file commands]
  (let [commands (cond (nil? commands)        []
                       (map? commands)        [commands]
                       (sequential? commands) commands
                       :else                  [commands])]
    (vec (keep-indexed (fn [i command] (command-problem file i command)) commands))))

(defn problem-line
  "The error line tern logs for one problem of command-problems, on the leg `leg`."
  [leg {:keys [file index command problem keys command-key]}]
  (format "tern: %s, %s command %d, %s: %s"
          file (name leg) (inc index) (pr-str command)
          (case problem
            :not-a-map        "is not a command map"
            :no-command-key   (str "has no command key (one of " (s/join " " command-keys) "), keys ()")
            :two-command-keys (str "has two command keys " (pr-str (seq keys))
                                   ", of which tern would run only the first")
            :unknown-keys     (str "has keys no generator reads for " command-key ": " (pr-str (seq keys))))))

(defn- exit!
  "Ends the process with `status`, as tern does when a migration cannot be run.
  A var, so that a test can trap the exit."
  [status]
  (System/exit status))

(defn check-commands!
  "Checks the leg `leg` (:up or :down) of the migration `file` (its name) before
  any of its SQL is generated: logs one error line per problem command-problems
  finds, naming the file, the command whole and the offending keys, then a line
  saying the migration was not run, and exits with status 1 (exit!), as tern's
  default generate-sql method ends on a command it does not know.  A key tern
  does not read would otherwise be ignored without a word, and a second command
  key would never run.  Returns nil when the leg has no problem."
  [file leg commands]
  (let [problems (command-problems file commands)]
    (when (seq problems)
      (doseq [problem problems]
        (log/error (problem-line leg problem)))
      (log/error (format "tern: %s was not run: fix the command%s of its %s leg above; no statement of it has run"
                         file (if (= 1 (count problems)) "" "s") (name leg)))
      (exit! 1))
    nil))

(defn run
  "Run the given migration: its :up leg, once check-commands! has passed it."
  [impl ^Path migration]
  (let [version  (parse-version migration)
        commands (with-open [^java.io.InputStream s (path->stream migration)]
                   (-> s slurp edn/read-string :up))]
    (check-commands! (fname migration) :up commands)
    (db/migrate impl version commands)))

(defn rollback
  "Roll back the given migration. Uses the same code as applying a migration,
  but simply passes the `down` commands and the version of the migration that's
  being rolled back to.

  The migration's own version is passed as `from` so that its row can be deleted
  from the version table.  Without it the rollback recorded nothing: the version
  it rolled back to was inserted a second time and the rolled-back version's row
  was left in place, so the database still reported itself at the version it had
  just undone.

  The :down leg is checked (check-commands!) before it runs, the :up leg not:
  only the leg about to run is checked."
  [impl ^Path migration version]
  (let [from     (parse-version migration)
        commands (with-open [s (path->stream ^Path migration)]
                   (-> s slurp edn/read-string :down))]
    (check-commands! (fname migration) :down commands)
    (db/migrate impl from version commands)))

(defn reset
  "Roll back ALL migrations."
  [{:keys [config] :as impl} version]
  (let [migrations (reverse (completed config version))]
    (doseq [^Path migration migrations]
      (rollback impl ^Path migration
                (previous-version config (parse-version ^Path migration))))))
