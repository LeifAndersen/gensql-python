(ns gensql.gateway
  (:require [gensql.query.permissive :as permissive]
            [gensql.query.strict :as strict]
            [gensql.query.db :as db])
  (:import [py4j GatewayServer])
  (:gen-class))

(defprotocol IGatewayEntryPoint
  (slurpDB [this pathB])
  (query [this text db])
  (queryString [this text db]))

(deftype GatewayEntryPoint []
  IGatewayEntryPoint
  (slurpDB [_ path] (atom (db/slurp path)))
  (query [_ text db] (permissive/query text db))
  (queryString [_ text db] (permissive/query text db)))

(defn -main []
  (let [gateway (GatewayServer. (->GatewayEntryPoint))]
    (.start gateway)
    (println "Running...")))
