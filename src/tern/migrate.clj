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

(defn run
  "Run the given migration."
  [impl ^Path migration]
  (let [version  (parse-version migration)
        commands (with-open [^java.io.InputStream s (path->stream migration)]
                   (-> s slurp edn/read-string :up))]
    (db/migrate impl version commands)))

(defn rollback
  "Roll back the given migration. Uses the same code as applying a migration,
  but simply passes the `down` commands and the version of the migration that's
  being rolled back to."
  [impl ^Path migration version]
  (let [commands (with-open [s (path->stream ^Path migration)]
                   (-> s slurp edn/read-string :down))]
        (db/migrate impl version commands)))

(defn reset
  "Roll back ALL migrations."
  [{:keys [config] :as impl} version]
  (let [migrations (reverse (completed config version))]
    (doseq [^Path migration migrations]
      (rollback impl ^Path migration
                (previous-version config (parse-version ^Path migration))))))
