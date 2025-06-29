;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.
;;
;; Copyright (c) KALEIDOS INC

 (ns app.main.ui.components.transactional
   "A wrapper for transactional inputs"
   (:require
    [app.common.schema :as sm]
    [app.common.uuid :as uuid]
    [app.main.data.workspace.undo :as dwu]
    [app.main.refs :as refs]
    [app.main.store :as st]
    [app.main.ui.components.numeric-input :refer [numeric-input*]]
    [app.main.ui.hooks :as h]
    [cljs.core :as c]
    [rumext.v2 :as mf]))

(def ^:private ^:const default-input-debounce-ms 800)

(def ^:private schema:transactional
  [:map {:closed false}
   [:on-change fn?]
   [:on-blur {:optional true} fn?]
   [:commit-on-blur {:optional true :default true} :boolean]
   [:debounce-ms {:optional true :default 800} :int]])

(def ^:private transactional*-validator
  (sm/lazy-validator schema:transactional))

(defn with-transactions
  "Wraps a component to provide transactional behavior.
   Changes are batched together and committed on blur or after a debounce period.
 
   Props:
    - :on-change - function used by the wrapped component to emit changes.
         This function should accept three arguments:
           1. The new value of the input
           2. The original event that triggered the change
           3. A boolean indicating whether the change is part of a transaction
         The latter is required for on-change functions which call `apply-modifiers`
    - :on-blur - function called when the input loses focus, may commit changes
    - :commit-on-blur - boolean indicating whether to commit changes on blur (default: true)
    - :debounce-ms - debounce time in milliseconds for committing changes
                      (default: 800ms, can be configured globally)
   "
  [component]
  (mf/fnc
   transactional*
   ;; FIXME: this does not work. How is schema validation supposed to work in this case?
   ;; {::mf/schema schema:transactional}
   [{:keys [on-change on-blur debounce-ms commit-on-blur]
     :or   {commit-on-blur true}
     :as   rest}]
   (let [debounce-ms    (or debounce-ms
                            (get refs/workspace-layout :input-debounce-ms default-input-debounce-ms))
         transaction-id (mf/use-var nil)
         local-value*   (mf/use-var nil) ;; Holds the current value of the input
         timer          (mf/use-var nil) ;; Timer for committing the transaction

         commit
         (mf/use-fn
          (mf/deps transaction-id timer)
          (fn []
            (when @timer
              (js/clearTimeout @timer))
            (reset! timer nil)
            (when @transaction-id
              (st/emit! (dwu/commit-undo-transaction @transaction-id))
              (reset! transaction-id nil))))

        ;; Handles changes to the input value, starting a transaction if needed.
        ;; Args:
        ;;   - value: the new value of the input
        ;;   - event: the JS event that triggered the change
         on-change'
         (mf/use-fn
          (mf/deps on-change transaction-id timer local-value*)
          (fn [value ^js event]
            (when (and value (not= @local-value* value))
              (when @timer
                (js/clearTimeout @timer))
              (reset! timer (js/setTimeout commit debounce-ms))
              (reset! local-value* value)
              (when (nil? @transaction-id)
                (reset! transaction-id (uuid/next))
                (st/emit! (dwu/start-undo-transaction @transaction-id {:timeout 0})))
              (on-change value event true))))

        ;; Blur events always commit the current transaction.
         on-blur'
         (mf/use-fn
          (mf/deps on-blur on-change' commit-on-blur)
          (fn [^js event]
            (when commit-on-blur
              (commit))
            (when (fn? on-blur)
              (on-blur event))))

         on-unmount (h/use-ref-callback commit)

         props (mf/spread-props rest {:on-change on-change' :on-blur on-blur'})]

    ;; Always commit on unmount
     (mf/with-effect [on-unmount] on-unmount)

     [:> component props])))

(def transactional-numeric-input*
  "A transactional numeric input component that wraps a numeric input with transactional behavior.
   
   Props:
    - :on-change: function used by the wrapped component to emit changes
    - :on-blur: function called when the input loses focus, may commit changes
    - :commit-on-blur: boolean indicating whether to commit changes on blur (default: true)
    - :debounce-ms: debounce time in milliseconds for committing changes
                      (default: 800ms, can be configured globally)
   "
  (with-transactions numeric-input*))