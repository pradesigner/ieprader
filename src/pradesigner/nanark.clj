"
identifies stocks common to ark and nanalyze
order is in ark preference
"

(ns user
  (:require [tablecloth.api :as tc]
            [clojure.string :as s]))

(def ark (-> (tc/dataset "resources/ark.txt"
                         {:dataset-name "ark"
                          :header-row? false
                          :column-blacklist ["column-1"]})
             (tc/rename-columns {"column-0" :ticker})))
(tc/row-count ark)
;; => 161

(def nan (-> (tc/dataset "resources/nan.csv"
                         {:dataset-name "nan"
                          :header-row? false})
             (tc/select-columns ["column-9"])
             (tc/drop-rows [0])
             (tc/rename-columns {"column-9" :ticker})))

(tc/row-count nan)
;; => 326

(def nanark (-> (tc/dataset)
                (tc/add-column :nan (tc/column nan :ticker))
                (tc/add-column :ark (tc/column ark :ticker) :na)
                (tc/add-column :common ((tc/inner-join ark nan :ticker)
                                        :ticker)
                               :na)))

;; a convoluted way to sort
(-> (tc/dataset)
        (tc/add-column :arknan (tc/column (-> (tc/inner-join ark nan :ticker)
                                          (tc/group-by :ticker)
                                          (tc/ungroup {:order? true}))
                                          :ticker))
        (tc/add-column :nanark (tc/column (-> (tc/inner-join nan ark :ticker)
                                              (tc/group-by :ticker)
                                              (tc/ungroup {:order? true}))
                                          :ticker)))

;; sort
(def nanarks (-> (tc/semi-join ark nan :ticker)
                 (tc/order-by :ticker)))

(seq (nanarks :ticker))

(binding [tech.v3.dataset.print/*default-table-row-print-length* 66]
  nanarks) ;still only 25

(binding [tech.v3.dataset.print/*default-table-row-print-length* 66]
  (println nanarks)) ;prints all

(seq (nanarks :ticker))

(defn set-data-print-length! [n]
   (alter-var-root #'tech.v3.dataset.print/*default-table-row-print-length* (fn [_] n)))
(set-data-print-length! 66) ;prints all




;; * joins

(def x (tc/dataset {:a [1 2 3 4]}))
(def y (tc/dataset {:a [5 3 4 6]}))
(tc/semi-join x y :a)
;; ** left-right-full
(tc/left-join x y :a)
;; => left-outer-join [4 2]:

| :a | :right.a |
|---:|---------:|
|  3 |        3 |
|  4 |        4 |
|  1 |          |
|  2 |          |

(tc/right-join x y :a)
;; => right-outer-join [4 2]:

| :a | :right.a |
|---:|---------:|
|  3 |        3 |
|  4 |        4 |
|    |        5 |
|    |        6 |

(tc/full-join x y :a)
;; => full-join [6 2]:

| :a | :right.a |
|---:|---------:|
|  3 |        3 |
|  4 |        4 |
|  1 |          |
|  2 |          |
|    |        5 |
|    |        6 |


;; ** intersections
(tc/inner-join x y :a)
;; => inner-join [2 1]:

| :a |
|---:|
|  3 |
|  4 |

(tc/semi-join x y :a);; => semi-join [2 1]:

| :a |
|---:|
|  3 |
|  4 |

(tc/intersect (tc/select-columns x :a)
              (tc/select-columns y :a))
;; => intersection [2 1]:

| :a |
|---:|
|  3 |
|  4 |


;; ** anti
(tc/anti-join x y :a)
;; => anti-join [2 1]:

| :a |
|---:|
|  1 |
|  2 |

(tc/anti-join y x :a)
;; => anti-join [2 1]:

| :a |
|---:|
|  5 |
|  6 |

(tc/difference (tc/select-columns x :a)
               (tc/select-columns y :a))
;; => difference [2 1]:

| :a |
|---:|
|  1 |
|  2 |

(tc/difference (tc/select-columns y :a)
               (tc/select-columns x :a))
;; => difference [2 1]:

| :a |
|---:|
|  5 |
|  6 |


;; ** building a new dataset with inner join
(-> (tc/dataset)
    (tc/add-column :c (tc/column x :a))
    (tc/add-column :d (tc/column y :a))
    (tc/add-column :inner ((tc/inner-join x y :a) ;get the inner ds
                           :a) ;turn into seq
                   :na) ;deal with shortness
    )


nanarks
