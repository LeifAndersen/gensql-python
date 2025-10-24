(ns gensql.gateway
   (:require [gensql.query.permissive :as permissive]
             [gensql.query.strict :as strict]
             [gensql.query.db :as db])
   (:gen-class
    :name gensql.gateway.Gateway
    :main false
    :methods [#^{:static true} [slurpDB [String] Object]
              #^{:static true} [query [String Object] Object]
              #^{:static true} [queryStrict [String Object] Object]]))

 (defn -slurpDB [path] 
   (atom (db/slurp path)))
 (defn -query [text db] 
   (permissive/query text db))
 (defn -queryStrict [text db]
   (strict/query text db))
