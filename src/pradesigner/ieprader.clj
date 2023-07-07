"
displays and summarizes results from ieprader
"

(ns pradesigner.ieprader
  (:require [tablecloth.api :as tc]
            [clojure.java.io :refer [file]]
            [clojure.string :as s]
            [clojure.java.shell :refer [sh]])
  (:gen-class))


(def fpaths
  "defines the file paths for AccountHoldings"
  (let [base "/home/pradmin/dd/"
        fns (filter #(re-find #"^Account" %)
                    (seq (.list (file base))))]
    (map #(str base %) fns)))

(defn remove-commas
  "removes commas from numbers for conversion purposes"
  [ds col]
  (map #(s/replace % #"," "") (ds col)))

(defn currency-format
  "takes to 2 decimal places"
  [ds col]
  (map #(format "%.2f" %) (ds col)))

(defn mk-ds
  "makes dataset from filepath"
  [fp]
  (tc/dataset fp
              {:header-row? false
               :key-fn keyword}))

(defn get-acct
  "gets the account number from the dataset"
  [ds]
  (->> (first (ds :column-0))
       (re-find #"(^.+) -")
       (second)))

(defn prep-columns
  "remove extraneous columns and rename existing ones"
  [ds]
  (-> ds
      (tc/rename-columns {:column-2 :ticker
                          :column-5 :qty
                          :column-6 :marketvalue
                          :column-7 :bookvalue
                          :column-8 :diff
                          :column-9 :%diff
                          :column-10 :%port})
      (tc/select-rows (comp #(= % "Equities") :column-0))
      (tc/drop-columns (for [n [0 1 3 4 11 12 13 14 15]]
                         (keyword (str "column-" n))))))

(defn str->num
  "convert from string to numbers"
  [ds]
  (-> ds
      (tc/add-or-replace-columns {:marketvalue (remove-commas ds :marketvalue)
                                  :bookvalue (remove-commas ds :bookvalue)
                                  :diff (remove-commas ds :diff)})
      (tc/convert-types {:qty :int16
                         :marketvalue :float64
                         :bookvalue :float64
                         :diff :float64
                         :%diff :float64
                         :%port :float64})))

(defn sumfn
  "sums items using reduce"
  [itm]
  (reduce + itm))

(defn prep-one-ds
  "prepares items from a single ds given filepath"
  [fp cols-to-sum]
  (let [ds-org (mk-ds fp)
        acct-info (get-acct ds-org)
        preped-ds (-> ds-org
                      (prep-columns)
                      (str->num))
        sums-of-cols (tc/aggregate-columns preped-ds
                                           cols-to-sum
                                           sumfn)
        summed-ds (tc/bind preped-ds sums-of-cols)]
    [acct-info sums-of-cols summed-ds]))

(defn prep-all-ds
  "puts all info into a sequence for further processing"
  [fps cols-to-sum]
  (let [all-data (map #(prep-one-ds % cols-to-sum) fps)
        col-sums (apply tc/bind (map second all-data))
        tot-col-sums (tc/aggregate-columns col-sums
                                           cols-to-sum
                                           sumfn)
        joined-data (for [e all-data]
                      (str "\n" (first e) "\n"
                           (s/replace (last e)
                                      #"null \[.+\]:\n\n"
                                      "")))
        final (str (s/replace tot-col-sums
                              #"_unnamed .+:\n\n"
                              "grand totals:\n")
                   "\n\n"
                   (apply str joined-data))]
    final))


(defn -main
  "runs get-group-tots"
  [& args]
  (spit "/home/pradmin/clj/ieprader/out.txt" (prep-all-ds fpaths [:marketvalue :bookvalue :diff]))
  (sh "emacs" "/home/pradmin/clj/ieprader/out.txt"))
