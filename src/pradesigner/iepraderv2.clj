"
displays and summarizes results from ieprader
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

(defn format->currency ;;TURNING BACK INTO STRING!!!!
  "format specific columns to currency (note using fn to save repetition)"
  [ds]
  (-> ds
      (tc/add-or-replace-columns (apply merge
                                        (map (fn [k] {k (currency-format ds k)})
                                             [:marketvalue :bookvalue :diff])))
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
        new-ds (-> ds
                   (prep-columns)
                   (tc/add-column :acct acct)
                   (tc/reorder-columns [:acct]))
        ;; ds-str (-> ds
        ;;            (prep-columns)
        ;;            (str->num)
        ;;            (concat-totals)
        ;;            (format->currency)
        ;;            (remove-ds-header))
        ]
;;    (str acct "\n\n" ds-str "\n\n\n\n")
    new-ds))

(defn mk-one-ds
  "concats all ds"
  [ds]
  (let [acct (get-acct ds)
        new-ds (-> ds
                   (prep-columns)
                   (str->num)
                   ;;(concat-totals)
                   ;;(format->currency)
                   (tc/add-column :acct acct)
                   (tc/reorder-columns [:acct]))]
    new-ds))

(defn join-all-ds
  "joins up all ds"
  [fp]
  (loop [acc (tc/dataset nil)
         fps fp]
    (if (empty? fps)
      acc
      (recur (tc/concat acc (mk-one-ds (mk-ds (first fps))))
             (rest fps)))))

(defn get-group-tots
  "produces a vector [total of totals, all group totals]"
  [ds group-by-cols cols-to-sum]
  (let [;;the summing function
        sumfn #(reduce + %)

        ;;the grouped ds
        g-ds (tc/group-by ds
                           group-by-cols
                           {:result-type :as-seq})

        ;;the group totals for each group
        group-sums (for [g g-ds]
                     (tc/aggregate-columns g
                                            cols-to-sum
                                            sumfn))

        ;;the group totals bound together into single dataset 
        summed-groups (apply tc/bind group-sums)

        ;;the total of the summed-groups dataset
        group-totals (tc/aggregate-columns summed-groups
                                            cols-to-sum
                                            sumfn)
        
        ;;the column sums attached to each group
        column-sums (map tc/bind g-ds group-sums)]

    ;;vector containing grouped dataset with
    ;;column sums and the column sums total
    [column-sums group-totals]
    ))


(get-group-tots (join-all-ds fpaths) [:acct] [:diff])
;; => [(null [9 8]:

|           :acct | :ticker | :qty | :marketvalue | :bookvalue |   :diff | :%diff | :%port |
|-----------------|---------|-----:|-------------:|-----------:|--------:|-------:|-------:|
| 58748842 prader |    TWOU |   20 |      1043.66 |     908.52 |  135.14 |  14.88 |   0.30 |
| 58748842 prader |     DDD |   40 |      1904.76 |    1879.01 |   25.75 |   1.37 |   0.55 |
| 58748842 prader |     MMM |   30 |      7292.79 |    7486.89 | -194.10 |  -2.59 |   2.12 |
| 58748842 prader |    DOCU |   20 |      7055.56 |    6853.15 |  202.41 |   2.95 |   2.05 |
| 58748842 prader |    VEGN |   30 |      1469.13 |    1414.78 |   54.35 |   3.84 |   0.43 |
| 58748842 prader |    SKLZ |  100 |      2748.00 |    2941.29 | -193.29 |  -6.57 |   0.80 |
| 58748842 prader |    TDOC |   40 |      8500.00 |    7425.69 | 1074.31 |  14.47 |   2.47 |
| 58748842 prader |    TSLA |   12 |     10130.35 |    9748.45 |  381.90 |   3.92 |   2.94 |
|                 |         |      |              |            | 1486.47 |        |        |
 null [4 8]:

|             :acct | :ticker | :qty | :marketvalue | :bookvalue |  :diff | :%diff | :%port |
|-------------------|---------|-----:|-------------:|-----------:|-------:|-------:|-------:|
| 59481160 lif_prad |       T |  200 |      7108.20 |    7074.75 |  33.45 |   0.47 |  10.69 |
| 59481160 lif_prad |     ENB |  200 |      9880.00 |    9534.95 | 345.05 |   3.62 |  14.85 |
| 59481160 lif_prad |     IBM |   44 |      7941.74 |    7988.89 | -47.15 |  -0.59 |  11.94 |
|                   |         |      |              |            | 331.35 |        |        |
 null [11 8]:

|                 :acct | :ticker | :qty | :marketvalue | :bookvalue |   :diff | :%diff | :%port |
|-----------------------|---------|-----:|-------------:|-----------:|--------:|-------:|-------:|
| 60359386 tfsa_ranjana |    ARKX |   40 |      1051.60 |    1021.27 |   30.33 |   2.97 |   1.18 |
| 60359386 tfsa_ranjana |    ARKQ |   10 |      1080.73 |    1010.73 |   70.00 |   6.93 |   1.21 |
| 60359386 tfsa_ranjana |    ARKW |   20 |      3867.06 |    3389.20 |  477.86 |  14.10 |   4.32 |
| 60359386 tfsa_ranjana |    ARKF |   20 |      1358.64 |    1232.30 |  126.34 |  10.25 |   1.52 |
| 60359386 tfsa_ranjana |    ARKG |   10 |      1153.89 |    1038.80 |  115.09 |  11.08 |   1.29 |
| 60359386 tfsa_ranjana |    ARKK |   30 |      4869.06 |    4134.07 |  734.99 |  17.78 |   5.44 |
| 60359386 tfsa_ranjana |      SQ |   30 |      9258.57 |    8836.50 |  422.07 |   4.78 |  10.35 |
| 60359386 tfsa_ranjana |    TRMB |   40 |      4103.68 |    3967.05 |  136.63 |   3.44 |   4.59 |
| 60359386 tfsa_ranjana |    PATH |   66 |      5639.11 |    5816.30 | -177.19 |  -3.05 |   6.30 |
| 60359386 tfsa_ranjana |       U |   33 |      4536.28 |    4559.57 |  -23.29 |  -0.51 |   5.07 |
|                       |         |      |              |            | 1912.83 |        |        |
 null [11 8]:

|               :acct | :ticker | :qty | :marketvalue | :bookvalue |   :diff | :%diff | :%port |
|---------------------|---------|-----:|-------------:|-----------:|--------:|-------:|-------:|
| 60359453 tfsa_kyron |    ARKX |   40 |      1051.60 |    1021.27 |   30.33 |   2.97 |   1.18 |
| 60359453 tfsa_kyron |    ARKQ |   10 |      1080.73 |    1009.63 |   71.10 |   7.04 |   1.21 |
| 60359453 tfsa_kyron |    ARKW |   20 |      3867.06 |    3386.75 |  480.31 |  14.18 |   4.34 |
| 60359453 tfsa_kyron |    ARKF |   20 |      1358.64 |    1232.06 |  126.58 |  10.27 |   1.52 |
| 60359453 tfsa_kyron |    ARKG |   10 |      1153.89 |    1038.18 |  115.71 |  11.15 |   1.29 |
| 60359453 tfsa_kyron |    ARKK |   30 |      4869.06 |    4135.53 |  733.53 |  17.74 |   5.46 |
| 60359453 tfsa_kyron |      SQ |   30 |      9258.57 |    8832.01 |  426.56 |   4.83 |  10.39 |
| 60359453 tfsa_kyron |    TRMB |   40 |      4103.68 |    3967.42 |  136.26 |   3.43 |   4.60 |
| 60359453 tfsa_kyron |    PATH |   66 |      5639.11 |    5816.30 | -177.19 |  -3.05 |   6.33 |
| 60359453 tfsa_kyron |       U |   33 |      4536.28 |    4563.37 |  -27.09 |  -0.59 |   5.09 |
|                     |         |      |              |            | 1916.10 |        |        |
 null [11 8]:

|              :acct | :ticker | :qty | :marketvalue | :bookvalue |   :diff | :%diff | :%port |
|--------------------|---------|-----:|-------------:|-----------:|--------:|-------:|-------:|
| 60359390 tfsa_prad |    ARKX |   40 |      1051.60 |    1021.27 |   30.33 |   2.97 |   1.17 |
| 60359390 tfsa_prad |    ARKQ |   10 |      1080.73 |    1009.87 |   70.86 |   7.02 |   1.20 |
| 60359390 tfsa_prad |    ARKW |   20 |      3867.06 |    3387.73 |  479.33 |  14.15 |   4.31 |
| 60359390 tfsa_prad |    ARKF |   20 |      1358.64 |    1231.81 |  126.83 |  10.30 |   1.51 |
| 60359390 tfsa_prad |    ARKG |   10 |      1153.89 |    1038.80 |  115.09 |  11.08 |   1.29 |
| 60359390 tfsa_prad |    ARKK |   30 |      4869.06 |    4135.90 |  733.16 |  17.73 |   5.43 |
| 60359390 tfsa_prad |      SQ |   30 |      9258.57 |    8828.52 |  430.05 |   4.87 |  10.32 |
| 60359390 tfsa_prad |    TRMB |   40 |      4103.68 |    3970.04 |  133.64 |   3.37 |   4.57 |
| 60359390 tfsa_prad |    PATH |   66 |      5639.11 |    5816.30 | -177.19 |  -3.05 |   6.28 |
| 60359390 tfsa_prad |       U |   33 |      4536.28 |    4561.12 |  -24.84 |  -0.55 |   5.06 |
|                    |         |      |              |            | 1917.26 |        |        |
) _unnamed [1 1]:

|   :diff |
|--------:|
| 7564.01 |
]



(defn do-printout
  "produces the output of the results"
  [[column-sums group-totals]]
  (let [colsums (for [cs column-sums]
                  (-> cs
                      tc/dataset->str
                      (clojure.string/replace #".+\n\n" "")))
        grtots (-> group-totals
                   tc/dataset->str
                   (clojure.string/replace #".+\n\n" ""))]
    (prn colsums "\n\n" grtots)))


(do-printout (get-group-tots (join-all-ds fpaths) [:acct] [:diff]))

(defn -main
  "runs get-group-tots"
  [& args]
  (prn (get-group-tots (join-all-ds fpaths) [:acct] [:diff])))





