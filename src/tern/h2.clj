(ns tern.h2
  (:require [tern.db :refer :all]
            [tern.h2v1]
            [tern.h2v2]
            [clojure.java.jdbc :as jdbc]))

(defn- get-version
  [db]
  (:v (jdbc/query
       (db-spec db)
       ["SELECT h2version() as v FROM dual"]
       :result-set-fn first)))

(defn get-migrator 
  [config]
  (let [version (get-version (:db config))
        [_ major-version] (re-matches #"(\d+)\.\d+.\d+.*" version)]
    (case major-version
      "1" (tern.h2v1/->H2V1Migrator config)
      "2" (tern.h2v2/->H2V2Migrator config))
    ))

(defrecord H2Migrator [config]
  Migrator
  (init[this]
    (let [migrator (get-migrator config)]
      (init migrator)))
  (version [this]
    (let [migrator (get-migrator config)]
      (version migrator)))
  (versions [this]
    (let [migrator (get-migrator config)]
      (versions migrator)))
  (migrate [this version commands]
    (let [migrator (get-migrator config)]
      (migrate migrator version commands))))
