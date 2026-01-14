(ns my-test.adversarial
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [jepsen.generator :as gen]))

(def ^:private default-profile-path "target_profile.json")
(def ^:private default-key-space 1000)

(defn- parse-target-key [value]
  (cond
    (nil? value) nil
    (number? value) (long value)
    (string? value) (try
                      (Long/parseLong value)
                      (catch Exception _ value))
    :else value))

(defn- read-profile [path]
  (try
    (let [file (io/file path)]
      (when (.exists file)
        (let [raw (slurp file)
              trimmed (str/trim raw)]
          (when (seq trimmed)
            (let [data (json/read-str trimmed :key-fn keyword)]
              (when (map? data) data))))))
    (catch Exception _ nil)))

(defn- uniform-key []
  (inc (rand-int default-key-space)))

(defn- txn-op [k]
  {:type :invoke
   :f :txn
   :value [[:r k nil] [:w k (System/nanoTime)]]})

(defn- txn-generator [key-fn]
  (gen/seq (repeatedly #(txn-op (key-fn)))))

(defn adversarial-gen
  ([] (adversarial-gen default-profile-path))
  ([profile-path]
   (let [profile (read-profile profile-path)
         target (parse-target-key (:target_key profile))]
     (if target
       (gen/frequency [[9 (txn-generator (constantly target))]
                       [1 (txn-generator uniform-key)]])
       (txn-generator uniform-key)))))
