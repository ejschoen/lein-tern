(ns tern.migrate-test
  (:require [tern.migrate :refer :all]
            [expectations :refer :all]))

(expect "20150107171132"
        (version {:migration-dir "examples/postgres-project/migrations"}))
