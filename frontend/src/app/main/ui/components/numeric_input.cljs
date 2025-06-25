;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.
;;
;; Copyright (c) KALEIDOS INC

 (ns app.main.ui.components.numeric-input
   "A numeric input component with support for mouse wheel input.

    Supported props:
      - `:value` - the current value of the input
      - `:on-change` - a function to call when the value changes
      - `:min` - minimum value (optional)
      - `:max` - maximum value (optional)
      - `:step` - step size for incrementing/decrementing (default: 1)
      - `:data-wrap` - whether to wrap the value around when it exceeds min/max
      - `:drag-direction` - direction for dragging (:ew, :we, :ns, :sn, :rotate)
      - `:disabled?` - whether the input is disabled (default: false)
      - `:select-on-focus?` - whether to select the input value on focus (default: true)
      - `:nillable?` - whether the input can be set to nil (default: false)
      - `:title`
      - ...

    Global configuration:
      - `:numeric-input-step` - default step size for numeric inputs (default: 1)
      - `:numeric-input-wheel-step` - step size for mouse wheel input (default: 1)
      - `:input-drag-sensitivity` - sensitivity for drag input (default: 1)
      - `:input-debounce-ms` - debounce time (in ms) for input changes. Applies
                               to wheel, dragging and kbdup/down (default: 800)
    "
   (:require
    ["rxjs" :as rxjs]
    [app.common.data :as d]
    [app.common.data.macros :as dm]
    [app.common.geom.point :as gpt]
    [app.common.schema :as sm]
    [app.common.uuid :as uuid]
    [app.main.data.workspace.undo :as dwu]
    [app.main.refs :as refs]
    [app.main.store :as st]
    [app.main.ui.css-cursors :as cur]
    [app.main.ui.formats :as fmt]
    [app.util.dom :as dom]
    [app.util.dom.normalize-wheel :as nw]
    [app.util.globals :as globals]
    [app.util.keyboard :as kbd]
    [app.util.mouse :as mse]
    [app.util.object :as obj]
    [app.util.simple-math :as smt]
    [beicon.v2.core :as rx]
    [cljs.core :as c]
    [cuerdas.core :as str]
    [potok.v2.core :as ptk]
    [rumext.v2 :as mf]))

(def ^:private ^:const default-input-debounce-ms 800)
(def ^:private ^:const default-input-drag-sensitivity 1)
(def ^:private ^:const default-input-large-step 10)
(def ^:private ^:const default-input-small-step 0.1)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; TODO: Remove once the two PRs in beicon are merged

(def ^function pairwise*
  rxjs/pairwise)

(defn pairwise
  "Groups pairs of consecutive emissions together and emits them in tuples."
  [ob]
  (rx/pipe (pairwise*) ob))


(defn from-event
  "Creates an Observable by attaching an event listener to an event target
   Args:
      et:   the event target (e.g., a DOM element or window)
      ev:   the event type (e.g., 'click', 'mousemove')
      opts: optional options for the event listener (e.g., {passive: true})"
  ([et ev & [opts]]
   (rxjs/fromEvent et ev (clj->js (or opts {})))))

(def ^function start-with*
  rxjs/startWith)

(defn start-with
  "Emits the provided value before any other emissions from the source Observable."
  [& args]
  (let [values (butlast args)
        ob     (last args)]
    (rx/pipe (apply start-with* values) ob)))

(def ^function end-with*
  rxjs/endWith)

(defn end-with
  "Emits the provided value(s) after all other emissions from the source Observable."
  [& args]
  (let [values (butlast args)
        ob     (last args)]
    (rx/pipe (apply end-with* values) ob)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; TODO more beicon PRs

(def ^function window*
  rxjs/window)

(defn window
  "Groups emissions from the source Observable into windows, each containing
   emissions until the next emission from the source Observable.
   Args:
      boundaries: an Observable that emits when to close the current window
      ob:         the source Observable to group into windows "
  [boundaries ob]
  (rx/pipe (window* boundaries) ob))


(def ^function exhaust-map*
  rxjs/exhaustMap)

(defn exhaust-map
  "Maps each value from the source Observable to an Observable, but ignores
   subsequent values until the inner Observable completes.
   Args:
     f: a function that takes a value and returns an Observable
     ob: the source Observable "
  [f ob]
  (rx/pipe (exhaust-map* f) ob))


(def ^function repeat*
  rxjs/repeat)

(defn rx-repeat
  "Repeats the source Observable indefinitely.
   Args:
     ob: the source Observable"
  [ob]
  (rx/pipe (repeat*) ob))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- cursor-from-direction
  "Returns the cursor class name based on the direction keyword.

   Args:
     direction - keyword indicating direction (:ew, :ns, :rotate)

   Returns a string representing the cursor class name."
  [direction]
  (case direction
    :ew (cur/get-dynamic "resize-ew" 0)
    :we (cur/get-dynamic "resize-ew" 180)
    :ns (cur/get-dynamic "resize-ns" 0)
    :sn (cur/get-dynamic "resize-ns" 180)
    :rotate (cur/get-dynamic "resize-ew" 180)
    (cur/get-static "default")))

(defn set-drag-cursor
  "Sets the cursor style for dragging operations by toggling a class on <body>.

   THIS IS A HACK: we need a better way to handle disabling the viewport
   cursor. The idea is to set a class on the body element and to deactivate
   custom cursors on the viewport when dragging.

   This works only with some temporary changes to viewport.cljs/scss and hooks.cljs

   Args:
     direction - (optional) keyword indicating direction (:ew, :ns, :rotate)"
  [direction]
  (ptk/reify ::set-drag-cursor
    ptk/EffectEvent
    (effect [_ _ _]
      (let [body (.-body globals/document)
            classes (.-classList body)
            class (cursor-from-direction direction)]
        (doseq [c (filter #(str/starts-with? % "cursor-") classes)]
          (.remove classes c))
        (when (not= class "default")
          (.add (.-classList body) class))))))

(defn- dim-from-direction
  "Given a direction keyword, returns a function that extracts the relevant
   coordinate from a point map for drag operations. For right-to-left (:we)
   and bottom-to-top (:sn) directions, the value is negated to reflect the
   reversed movement.

   Args:
     direction - keyword indicating direction (:ew, :we, :ns, :sn, :rotate)

   Returns:
     A function that takes a point and returns the value (possibly negated)
     of the corresponding dimension for drag calculations."
  [direction]
  (case direction
    :ew (fn [pt] (dm/get-prop pt :x))
    :we (fn [pt] (* -1 (dm/get-prop pt :x)))
    :ns (fn [pt] (dm/get-prop pt :y))
    :sn (fn [pt] (* -1 (dm/get-prop pt :y)))
    :rotate (fn [pt] (dm/get-prop pt :x))
    (fn [pt] (dm/get-prop pt :x))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Value handling

(defn- parse-value*
  "Parses the value from the input element, applying min and max constraints,
   and returning a default value if the input is nillable or invalid.

   This also evaluates simple math expressions in the input value.

   Args:
     ref:       a reference to the input DOM element
     min-value: minimum allowed value
     max-value: maximum allowed value
     initial:   the initial value of the input
     nillable?: whether the input can be nillable
     default:   default value to return if parsing fails or is nillable

   Returns:
     A number within the specified range, or the default value if parsing fails."
  [ref min-value max-value ^atom initial nillable? default]
  (mf/use-fn
   (mf/deps ref min-value max-value initial nillable? default)
   (fn []
     (when-let [node (mf/ref-val ref)]
       (let [new-value (-> (dom/get-value node)
                           (str/strip-suffix ".")
                           (smt/expr-eval @initial))]
         (cond
           (d/num? new-value)
           (-> new-value
               (d/max (/ sm/min-safe-int 2))
               (d/min (/ sm/max-safe-int 2))
               (cond-> (d/num? min-value)
                 (d/max min-value))
               (cond-> (d/num? max-value)
                 (d/min max-value)))

           nillable?
           default

           :else @initial))))))

(defn- compute-delta*
  ""
  [wrap-value? min-value max-value step-value parse-value default]
  (fn [^js event ^boolean up? ^boolean down?]
    (let [current-value (parse-value)
          current-value (or current-value
                            (cond (and down? (d/num? max-value)) max-value
                                  (and up? (d/num? min-value)) min-value
                                  :else (d/nilv default 0)))

          big-step   (get refs/workspace-layout :input-large-step default-input-large-step)
          small-step (get refs/workspace-layout :input-small-step default-input-small-step)
          increment  (cond (kbd/shift? event) big-step
                           (kbd/alt? event) small-step
                           :else 1)
          increment  (* increment (if up? step-value (- step-value)))

          new-value (+ current-value increment)
          new-value (cond
                      (and wrap-value? (d/num? max-value min-value)
                           (> new-value max-value) up?)
                      (-> new-value (- max-value) (+ min-value) (- step-value))

                      (and wrap-value? (d/num? max-value min-value)
                           (< new-value min-value) down?)
                      (-> new-value (- min-value) (+ max-value) (+ step-value))

                      (and (d/num? min-value) (< new-value min-value))
                      min-value

                      (and (d/num? max-value) (> new-value max-value))
                      max-value

                      :else new-value)]
      new-value)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Input streams

(defn- drag-stream
  ""
  [node compute-delta direction]
  (let [sensitivity (get refs/workspace-layout :input-drag-sensitivity default-input-drag-sensitivity)
        get-dim     (dim-from-direction direction)

        make-change
        (fn [[prev curr]]
          (let [prev-pos (dom/get-client-position prev)
                curr-pos (dom/get-client-position curr)
                delta    (gpt/subtract curr-pos prev-pos)
                diff     (get-dim delta)
                up?      (> diff 0)
                down?    (< diff 0)
                value    (when (> (abs diff) sensitivity)
                           (compute-delta curr up? down?))]
            [value curr]))

        start-s
        (->> (from-event node "dragstart" #js {:passive false})
                      ;; (rx/tap #(js/console.log "drag-stream: dragstart" %))
             (rx/tap dom/prevent-default)
             (rx/tap dom/stop-propagation)
             (rx/share))
        end-s
        (rx/share
         (rx/merge
          (->> (rx/merge
                (from-event node "dragend")
                (from-event globals/window "pointerup")
                (->> (from-event node "keydown")
                     (rx/filter kbd/esc?)))
               (rx/tap #(js/console.log "drag-stream: event type" (.-type ^js %)))
               (rx/tap dom/prevent-default)
               (rx/tap dom/stop-immediate-propagation))
          (mse/drag-stopper st/stream)))

        ;; Builds the inner stream that emits drag events
        make-drag-s
        (fn [_start-ev]
          (let [move-s  (->> (from-event globals/window "pointermove")
                             (pairwise)
                             (rx/share))
                begin   (->> move-s
                             (rx/first)
                              ;; (rx/tap #(js/console.log "drag-stream: pointermove" %))
                             (rx/map make-change)
                             (rx/tap #(set-drag-cursor direction)))
                updates (->> move-s
                             (rx/map make-change))
                end     (->> end-s
                             (rx/first)
                             (rx/tap #(set-drag-cursor nil))
                             (rx/tap #(dom/select-text! node)))]

            (rx/concat
             (->> (rx/merge begin updates)
                  (rx/take-until end-s))
             end)))]
    (exhaust-map make-drag-s start-s)))

(defn- keyboard-stream
  ""
  [node compute-delta initial-value* parse-value]
  (let [up?      #(or (kbd/up-arrow? %) (kbd/page-up? %))
        down?    #(or (kbd/down-arrow? %) (kbd/page-down? %))
        is-step? #(or (up? %) (down? %))
        accept?  #(or (kbd/enter? %) (kbd/tab? %))
        cancel?  kbd/esc?

        ;; raw keydown stream (cold)
        event-s    (rx/share (from-event node "keydown" #js {:passive false}))

        step-s (->> event-s
                    (rx/filter is-step?)
                    (rx/tap dom/prevent-default)
                    (rx/tap dom/stop-propagation)
                    (rx/map (fn [e] [(compute-delta e (up? e) (down? e)) e])))

        accept-s (->> event-s
                      (rx/filter accept?)
                      (rx/map (fn [e] [(parse-value) e])))

        cancel-s (->> event-s
                      (rx/filter cancel?)
                      ; This should both push the change and leave focus
                      ; triggering a transaction commit in transactional-input*
                      (rx/map (fn [e] [@initial-value* e])))]

    (rx/merge step-s accept-s cancel-s)))

(defn wheel-delta [^js event compute-delta]
  (let [event* ^js (nw/normalize-wheel event)
        delta-pt   (gpt/point (.-spinX event*) (.-spinY event*))
        get-dim    (dim-from-direction :sn)
        delta      (get-dim delta-pt)
        up?        (> delta 0)
        down?      (< delta 0)]
    (compute-delta event up? down?)))

(defn wheel-stream
  ""
  [node compute-delta]
  (let [make-change (fn [^js event]
                      [(wheel-delta event compute-delta) event])]
    (->> (from-event node "wheel" #js {:passive false})
         (rx/tap dom/prevent-default)
         (rx/tap dom/stop-immediate-propagation)
         (rx/tap dom/stop-propagation)
         (rx/map make-change))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Component

(mf/defc numeric-input*
  {::mf/wrap-props false
   ::mf/forward-ref true}
  [props external-ref]
  (let [value-str    (unchecked-get props "value")
        min-value    (unchecked-get props "min")
        max-value    (unchecked-get props "max")
        step-value   (unchecked-get props "step")
        wrap-value?  (unchecked-get props "data-wrap")

        ;; Accepts :ew, :we, :ns, :sn, :rotate
        drag-direction (keyword (unchecked-get props "dragDirection"))

        on-blur      (unchecked-get props "onBlur")
        on-focus     (unchecked-get props "onFocus")
        on-change    (unchecked-get props "onChange")

        title        (unchecked-get props "title")
        default      (unchecked-get props "default")
        nillable?    (unchecked-get props "nillable")
        class        (d/nilv (unchecked-get props "className") "")

        min-value    (d/parse-double min-value)
        max-value    (d/parse-double max-value)
        step-value   (d/parse-double step-value 1)
        default      (d/parse-double default (when-not nillable? 0))

        select-on-focus? (d/nilv (unchecked-get props "selectOnFocus") true)

        ;; We need a ref pointing to the input dom element, but the user
        ;; of this component may provide one (that is forwarded here).
        ;; So we use the external ref if provided, and the local one if not.
        local-ref   (mf/use-ref)
        ref         (or external-ref local-ref)

        ;; This represents the previous value and is used as
        ;; initial value for simple math expression evaluation.
        initial-value* (mf/use-var (when (not= :multiple value-str)
                                     (d/parse-double value-str default)))

        ;; Current value of the input, updated on-change, used to apply
        ;; unapplied changes upon unmount / blur
        curr-value* (mf/use-var @initial-value*)

        ;; Stream of changes (drag, wheel, keyboard)
        change-s   (mf/use-memo #(rx/subject))

        unmount-s (mf/use-memo #(rx/subject))


        ;; fn that parses the value from the input element, applying
        ;; min and max constraints, and returning a default value if the input
        ;; is nillable or invalid.
        parse-value (parse-value* ref min-value max-value initial-value* nillable? default)

        ;; Function that computes the delta value based on the current value,
        ;; min and max values, step value, and the parse function.
        ;; This is used for wheel, up/down and drag interactions.
        compute-delta (compute-delta* wrap-value? min-value max-value step-value parse-value default)

        ;; Called on every modification of the input value not handled via the change-s,
        ;; e.g. number input, paste, etc.
        handle-change
        (mf/use-fn
         (mf/deps parse-value)
         (fn [^js event]
           (when-let [new-value (parse-value)]
             (js/console.log "handle-change called with "
                             "curr-value*:" @curr-value*
                             "new-value:" new-value
                             "event:" (if event (.-type event) "nil"))
             (reset! curr-value* new-value))))

        ;; Called on changes induced by drag, wheel, keyboard up/down
        on-change'
        (mf/use-fn
         (mf/deps parse-value)
         (fn [new-value ^js event]
           (when new-value
             (reset! curr-value* new-value)
             (js/console.log "on-change' called with parse-value:" (parse-value)
                             "curr-value*:" @curr-value*
                             "event:" (if event (.-type event) "nil"))
             (when-let [node (mf/ref-val ref)]
               (dom/set-value! node (fmt/format-number new-value)))
             (on-change new-value event))))

        on-unmount
        (mf/use-fn
         (mf/deps unmount-s ref curr-value*)
         (fn [^js event]
           (st/emit! (set-drag-cursor nil))
           (when (not= @curr-value* @initial-value*)
             (rx/push! change-s [@curr-value* event]))
           ;; FIXME: Is this needed to free resources in change-s?
           ;; Should I rather call rx/end! on change-s?
           (.next unmount-s)))

        ;; Exits the input component, implicitly accepting the current value 
        on-blur'
        (mf/use-fn
         (mf/deps on-blur on-unmount)
         (fn [^js event]
           (on-unmount event)
           (when (fn? on-blur)
             (on-blur event))))

        on-focus'
        (mf/use-fn
         (mf/deps ref on-focus select-on-focus? on-change parse-value)
         (fn [event]
           (reset! curr-value* (parse-value))
           (reset! initial-value* @curr-value*)
           (when (fn? on-focus)
             (on-focus event))

           ;; only create streams upon focus and if the on-change handler is provided
           (when (fn? on-change)
             (let [input-node (mf/ref-val ref)
                   stopper    (rx/merge
                               unmount-s
                               (from-event input-node "blur" #js {:passive false :once true}))
                   wheel-s    (wheel-stream input-node compute-delta)
                   drag-s     (drag-stream input-node compute-delta drag-direction)
                   keyboard-s (keyboard-stream input-node compute-delta initial-value* parse-value)
                   change-s'  (->> (rx/merge wheel-s drag-s keyboard-s)
                                   (rx/take-until stopper))]
               (rx/sub! change-s' {:next (partial rx/push! change-s)
                                   :error (fn [e] (js/console.error "Error in change stream:" e))
                                   :complete #(js/console.log "Change stream completed")})))

           (when select-on-focus?
             (let [target (dom/get-target event)]
               (dom/select-text! target)
                ;; In webkit browsers the mouseup event will be called after the on-focus causing and unselect
               (.addEventListener target "mouseup" dom/prevent-default #js {:once true})))))

        props (-> (obj/clone props)
                  (obj/unset! "dragDirection")
                  (obj/unset! "selectOnFocus")
                  (obj/unset! "nillable")
                  (obj/set! "onChange" handle-change)
                  (obj/set! "value" mf/undefined)
                  (obj/set! "className" (str/join " " [class (cursor-from-direction drag-direction)]))
                  (obj/set! "type" "text")
                  (obj/set! "ref" ref)
                  (obj/set! "defaultValue" (fmt/format-number @initial-value*))
                  (obj/set! "title" title)
                  (obj/set! "onBlur" on-blur')
                  (obj/set! "onFocus" on-focus'))]

    (mf/with-effect [on-unmount ref]
      (when-let [input-node (mf/ref-val ref)]
        (dom/set-value! input-node (fmt/format-number @initial-value*)))
      #(on-unmount #js {:type "unmount"}))

    (mf/with-effect [change-s on-change']  ;
      (js/console.log "Subscribing to change stream")
      (let [subs [(rx/sub! change-s
                           (fn [[value ^js event]]
                             (js/console.log "change-s value:" value
                                             "event:" (if event (.-type event) "nil"))))
                  (rx/sub! change-s (partial apply on-change'))]]
        #(doseq [s subs]
           (rx/dispose! s))))

    [:> :input props]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Transactional Input wrapper

(def ^:private schema:transactional-input  ;TODO
  [:map
   [:label {:optional true} :string]
   [:class {:optional true} :string]])

;; FIXME: change this to uuid/next or js.Symbol
(defonce counter (atom 0))

(mf/defc transactional-input*
  "A component that wraps a numeric input allowing for transactional changes."
  {::mf/forward-ref true}
  ;;  ::mf/schema schema:transactional-input

  [{:keys [on-change on-blur] :rest props} ref]
  (let [debounce-ms    (get refs/workspace-layout :input-debounce-ms default-input-debounce-ms)
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
             (js/console.log "Committing transaction" (subs (str @transaction-id) 0 6))
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
           (js/console.log "on-change wrapper -- "
                           "value:" value
                           "local value:" @local-value*
                           "event:" (if event (.-type event) "nil")
                           "transaction-id:" (subs (str @transaction-id) 0 6)
                           "timer:" @timer)

           (when (and value (not= @local-value* value))
             (when @timer
               (js/clearTimeout @timer))
             (js/console.log "Setting new timer for commit")
             (reset! timer (js/setTimeout commit debounce-ms))

             (reset! local-value* value)
             (when (nil? @transaction-id)
               (reset! transaction-id (uuid/next))
               (js/console.log "Starting new transaction" (subs (str @transaction-id) 0 6))
               (st/emit! (dwu/start-undo-transaction @transaction-id {:timeout 0})))

             (js/console.log "Updating value in transaction" (subs (str @transaction-id) 0 6))
             (on-change value event true))))

        ;; Blur events always commit the current transaction.
        on-blur'
        (mf/use-fn
         (mf/deps on-blur on-change')
         (fn [^js event]
           (js/console.log "on-blur wrapper -- event:" (if event (.-type event) "nil"))
           (commit)
           (when (fn? on-blur)
             (on-blur event))))

        props (mf/spread-props props {:on-change on-change' :on-blur on-blur'})]

    ;; Always commit on unmount
    (mf/with-effect [commit] commit)

    [:> numeric-input* props]))

