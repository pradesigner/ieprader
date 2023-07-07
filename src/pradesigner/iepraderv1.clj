"
displays and summarizes results from ieprader

TODO
pull items for accumulations from each of the accounts - by merging them?
"

(ns pradesigner.ieprader
  (:require [tablecloth.api :as tc]
            [clojure.java.io :refer [file]])
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
  (map #(clojure.string/replace % #"," "") (ds col)))

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

(defn concat-totals
  "create totals from columns :qty :marketvalue :bookvalue :diff and concat to ds"
  [ds]
  (tc/concat ds
             (-> ds
                 (tc/aggregate-columns [:qty :marketvalue :bookvalue :diff] #(reduce + %))
                 (tc/add-columns {:%diff nil
                                  :%port nil
                                  :ticker "Totals"}))))

(defn format->currency
  "format specific columns to currency (note using fn to save repetition)"
  [ds]
  (-> ds
      (tc/add-or-replace-columns (apply merge
                                        (map (fn [k] {k (currency-format ds k)})
                                             [:marketvalue :bookvalue])))
      #_(tc/add-or-replace-columns {:marketvalue (currency-format ds5 :marketvalue)
                                    :bookvalue (currency-format ds5 :bookvalue)})))

(defn remove-ds-header
  "removes the ds header which shows filename and shape"
  [ds]
  (-> ds
      (tc/dataset->str)
      (clojure.string/replace #".+\n\n" "")))

(defn pr-ds
  "prepares ds given filepath"
  [fp]
  (let [ds (mk-ds fp)
        acct (get-acct ds)
        ds-str (-> ds
                   (prep-columns)
                   (str->num)
                   (concat-totals)
                   (format->currency)
                   (remove-ds-header))]
    (str acct "\n\n" ds-str "\n\n\n\n")))

(spit "resources/ieholdings.txt" (apply str (map pr-ds fpaths)))

;; * zz
(defn pr-ds
  "prepares dataset"
  [fp]
  (let [ds1 (mk-ds fp)
        acct (get-acct ds1)
        
        ;; remove extraneous columns and rename existing ones 
        ds2 (-> ds1
                (tc/rename-columns {:column-2 :ticker
                                    :column-5 :qty
                                    :column-6 :marketvalue
                                    :column-7 :bookvalue
                                    :column-8 :diff
                                    :column-9 :%diff
                                    :column-10 :%port})
                (tc/select-rows (comp #(= % "Equities") :column-0))
                (tc/drop-columns (for [n [0 1 3 4 11 12 13 14 15]]
                                   (keyword (str "column-" n)))))

        ;; convert from string to numbers
        ds3 (-> ds2
                (tc/add-or-replace-columns {:marketvalue (remove-commas ds2 :marketvalue)
                                            :bookvalue (remove-commas ds2 :bookvalue)})
                (tc/convert-types {:qty :int16
                                   :marketvalue :float64
                                   :bookvalue :float64
                                   :diff :float64
                                   :%diff :float64
                                   :%port :float64}))

        ;; create the totals
        ds4 (-> ds3
                (tc/aggregate-columns [:qty :marketvalue :bookvalue :diff] #(reduce + %))
                (tc/add-columns {:%diff nil
                                 :%port nil
                                 :ticker "Totals"}))

        ;; concat the totals row to the rest of the dataset
        ds5 (tc/concat ds3 ds4)

        ;; format specific columns to currency (note using fn to save repetition)
        ds6 (-> ds5
                (tc/add-or-replace-columns (apply merge
                                                  (map (fn [k] {k (currency-format ds5 k)})
                                                       [:marketvalue :bookvalue])))
                #_(tc/add-or-replace-columns {:marketvalue (currency-format ds5 :marketvalue)
                                              :bookvalue (currency-format ds5 :bookvalue)})
                )
        ds7 (-> ds6
                (tc/dataset->str)
                (clojure.string/replace #".+\n\n" ""))
        ]
    (str acct
         "\n\n"
         ds7
         "\n\n\n\n")))





(defn -main
  "I don't do a whole lot ... yet."
  [fp]
  (let [ds (mk-ds fp)])
  ((comp pr-ds mk-ds) (first fpaths)))

(-main)



