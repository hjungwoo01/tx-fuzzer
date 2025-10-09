(ns clj.run_elle
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.pprint :as pp]
            [elle.core :as elle]
            [jepsen.history :as h])
  (:gen-class))

;; --------------------------------
;; Read many EDN forms from a file
;; --------------------------------
(defn read-history
  "Reads a file containing *many* EDN forms (one per line or chunk) into a vector."
  [f]
  (with-open [r (java.io.PushbackReader. (io/reader f))]
    (loop [acc []]
      (let [form (edn/read {:eof ::eof} r)]
        (if (= form ::eof)
          acc
          (recur (conj acc form)))))))

;; -------------------------------
;; Helpers (define before use)
;; -------------------------------
(defn ->long
  "Coerce x into a Long where reasonable.
   Accepts numbers and strings like \"123\" or \"k123\"."
  [x]
  (cond
    (nil? x)     nil
    (integer? x) (long x)
    (float? x)   (long x)
    (string? x)  (let [s x]
                   (cond
                     (re-matches #"\d+" s) (Long/parseLong s)
                     (re-matches #"(?i)k\d+" s) (Long/parseLong (subs s 1))
                     :else nil))
    :else        nil))

(defn classify [x]
  (cond
    (nil? x)     :nil
    (number? x)  :number
    (keyword? x) :keyword
    (string? x)  :string
    (vector? x)  :vector
    (map? x)     :map
    :else        :other))

;; -------------------------------
;; Sub-op sanitization
;; -------------------------------
(defn sanitize-subop
  "Given a sub-op like [:r k] / [:r k v?] / [:w k v], ensure numeric k/v (or nil for read v).
   Returns {:status :ok :sub [...] } or {:status :bad :reason ... :sub sub}."
  [sub]
  (if (and (vector? sub) (seq sub))
    (let [[tag k v] sub]
      (cond
        (not (#{:r :w} tag))
        {:status :bad :reason (str "tag must be :r or :w, got " (pr-str tag)) :sub sub}

        ;; Allow 2-elt read → [:r k nil]
        (and (= tag :r) (= 2 (count sub)))
        (let [k' (->long k)]
          (if (some? k')
            {:status :ok :sub [tag k' nil]}
            {:status :bad :reason (str "read key not numeric: " (pr-str k)) :sub sub}))

        (< (count sub) 3)
        {:status :bad :reason (str "need 3 elts, got " (count sub)) :sub sub}

        :else
        (let [k' (->long k)
              v' (->long v)]
          (cond
            (nil? k') {:status :bad :reason (str "key not numeric: " (pr-str k)) :sub sub}
            (and (nil? v') (not (nil? v)))
            {:status :bad :reason (str "value not numeric: " (pr-str v)) :sub sub}
            :else {:status :ok :sub [tag k' v']}))))
    {:status :bad :reason (str "sub-op not a vector: " (pr-str sub)) :sub sub}))

(defn sanitize-value!
  "Sanitize the :value vector of a :txn op. Throws with detailed ex-data on first problem.
   On success, returns the op with :value replaced by sanitized sub-ops."
  [op]
  (let [v (:value op)]
    (when-not (vector? v)
      (throw (ex-info "op :value must be a vector of sub-ops"
                      {:op-summary (select-keys op [:type :process :time :index :f])
                       :value-type (type v)})))
    (loop [i 0 acc []]
      (if (= i (count v))
        (assoc op :value acc)
        (let [res (sanitize-subop (nth v i))]
          (if (= (:status res) :ok)
            (recur (inc i) (conj acc (:sub res)))
            (throw (ex-info "Bad sub-op"
                            {:op-summary (select-keys op [:type :process :time :index :f])
                             :sub-index i
                             :reason (:reason res)
                             :sub (:sub res)}))))))))

(defn sanitize-top-op
  "Ensure numeric :time/:process, add :index, and sanitize :value for :txn ops."
  [idx op]
  (let [t   (or (->long (:time op)) (->long (:wall op)) (long idx))
        p   (or (->long (:process op)) 0)
        op' (-> op
                (assoc :index (long idx))
                (assoc :time t)
                (assoc :process p))]
    (if (= (:f op') :txn)
      (sanitize-value! op')
      op')))

;; -------------------------------
;; Defensive scans
;; -------------------------------
(defn sweep-for-bad-kv
  "Scan all :txn sub-ops and report the first where k/v is a keyword/other.
   Also returns tallies for key/value types across the history."
  [ops]
  (let [bad    (atom nil)
        counts (atom {:k {:number 0 :keyword 0 :string 0 :nil 0 :other 0}
                      :v {:number 0 :keyword 0 :string 0 :nil 0 :other 0}})]
    (doseq [op ops
            :when (= (:f op) :txn)
            :let  [vals (:value op)]
            i (range (count vals))]
      (when-not @bad
        (let [sub (nth vals i)
              [tag k v] (when (vector? sub) sub)]
          (when (and (vector? sub) (#{:r :w} tag))
            (swap! counts update-in [:k (classify k)] inc)
            (swap! counts update-in [:v (classify v)] inc)
            (when (or (= (classify k) :keyword)
                      (= (classify v) :keyword)
                      (= (classify k) :other)
                      (= (classify v) :other))
              (reset! bad {:op        (select-keys op [:index :process :time :type :f])
                           :sub-index i
                           :sub       sub
                           :k-class   (classify k)
                           :v-class   (classify v)}))))))
    {:counts @counts :bad @bad}))

(defn sweep-all-for-bad-subops
  "Walk the entire op stream. If we see any vector that *looks* like an Elle sub-op
   (starts with :r or :w), validate it. Return first offending instance with context."
  [ops]
  (letfn [(looks-like-subop? [x]
            (and (vector? x) (seq x) (#{:r :w} (first x))))]
    (let [bad (atom nil)]
      (doseq [op ops
              :when (not @bad)]
        ;; Check :value inside :txn
        (when (= (:f op) :txn)
          (let [v (:value op)]
            (when (not (vector? v))
              (reset! bad {:where :txn-value-not-vector
                           :op (select-keys op [:index :process :time :f :type])
                           :value-type (type v)}))
            (doseq [i (range (count (or v [])))
                    :when (not @bad)]
              (let [sub (nth v i)]
                (when (looks-like-subop? sub)
                  (let [[tag k val] (concat sub [nil nil])]
                    (when (or (nil? k) (not (number? (->long k)))
                              (and (= tag :w)
                                   (not (or (nil? val) (number? (->long val))))))
                      (reset! bad {:where :txn-subop
                                   :op (select-keys op [:index :process :time :f :type])
                                   :sub-index i
                                   :sub sub})))))))
        ;; Also scan top-level values for stray Elle-like vectors
        (doseq [x (tree-seq coll? seq op)
                :when (and (not @bad) (looks-like-subop? x))]
          (let [[tag k val] (concat x [nil nil])]
            (when (or (nil? k) (not (number? (->long k)))
                      (and (= tag :w)
                           (not (or (nil? val) (number? (->long val))))))
              (reset! bad {:where :non-txn-subop
                           :op (select-keys op [:index :process :time :f :type])
                           :sub x})))))
      @bad)))

;; -------------------------------
;; Model runner
;; -------------------------------
(def candidate-models
  [:strict-serializable :serializable :snapshot-isolation :repeatable-read :read-committed])

(defn try-model! [hist m]
  (println "Calling elle.core/check with opts" {:consistency-models [m]} "…")
  (flush)
  (try
    (let [res (elle/check hist {:consistency-models [m]})]
      (println (str "\n=== ELLE ANALYSIS (" (name m) ") ==="))
      (pp/pprint (dissoc res :index))
      (flush)
      (when (seq (:anomalies res))
        (println "\nAnomalies detected.")
        (System/exit 3))
      :ok)
    (catch Throwable t
      (println "  -> failed:" (.getName (class t)) "-" (.getMessage t))
      nil)))

;; -------------------------------
;; Main
;; -------------------------------
(defn -main [& args]
  (println "[run-elle] starting, args =" (vec args))
  (let [file (first args)]
    (when-not file
      (println "Usage: make analyse HIST=<history.edn>")
      (System/exit 1))
    (println "[run-elle] reading history from" file)
    (let [raw (read-history file)]
      (when (empty? raw)
        (println "History is empty; nothing to analyze.")
        (System/exit 2))
      (println "Loaded" (count raw) "top-level records")
      ;; 1) sanitize top-level ops & index
      (let [ops1 (vec (map-indexed sanitize-top-op raw))]
        (println "Sample sanitized txn ops:"
                 (pr-str (->> ops1 (filter #(= (:f %) :txn)) (take 3))))
        ;; quick field summary
        (let [ft (fn [k] (->> ops1 (map k) (map classify) frequencies))]
          (println "Field type summary:")
          (println "  :time   " (ft :time))
          (println "  :process" (ft :process))
          (println "  :index  " (ft :index)))
        ;; defensive scans
        (when-let [off (sweep-all-for-bad-subops ops1)]
          (println "\n*** Offending sub-op found outside expectations ***")
          (pp/pprint off)
          (println "\nFix the emitter/sanitizer so all Elle-ish vectors have numeric k/v.")
          (System/exit 6))
        (let [{:keys [counts bad]} (sweep-for-bad-kv ops1)]
          (println "KV type tallies: keys" (:k counts) " values" (:v counts))
          (when bad
            (println "\n*** Offending sub-op (non-numeric/keyword) ***")
            (pp/pprint bad)
            (println "\nFix your emitter/sanitizer so k/v are numeric (or nil for read v).")
            (System/exit 5)))
        ;; 2) wrap to Jepsen history and try models
        (let [hist (h/history ops1)]
          (println "Wrapped into Jepsen history type:" (type hist))
          (flush)
          (if (some #(try-model! hist %) candidate-models)
            (do (println "\nAll good for at least one model.")
                (System/exit 0))
            (do (println "Elle check failed for all models")
                (System/exit 4))))))))
)