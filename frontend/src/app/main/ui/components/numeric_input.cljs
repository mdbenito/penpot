;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.
;;
;; Copyright (c) KALEIDOS INC

 (ns app.main.ui.components.numeric-input
   (:require
    ["rxjs" :as rxjs]
    [app.common.data :as d]
    [app.common.data.macros :as dm]
    [app.common.geom.point :as gpt]
    [app.common.schema :as sm]
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
    [goog.events :as events]
    [potok.v2.core :as ptk]
    [rumext.v2 :as mf]))


;; TODO: PR for beicon to add pairwise operator
(def ^function pairwise*
  rxjs/pairwise)

(defn pairwise
  "Groups pairs of consecutive emissions together and emits them in tuples."
  [ob]
  (rx/pipe (pairwise* ob) ob))

;; TODO: PR for beicon to extend fromEvent operator with options
(defn from-event
  "Creates an Observable by attaching an event listener to an event target
   Args:
      et:   the event target (e.g., a DOM element or window)
      ev:   the event type (e.g., 'click', 'mousemove')
      opts: optional options for the event listener (e.g., {passive: true})"
  ([et ev & [opts]]
   (rxjs/fromEvent et ev (clj->js (or opts {})))))

(defn simple-effect
  "Creates a simple effect that runs a function with optional arguments.
   Returns an EffectEvent that runs the function when triggered."
  [fun & [args]]
  (ptk/reify ::simple-effect
    ptk/EffectEvent
    (effect [_ _ _]
      (apply fun args))))

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
    :rotate (cur/get-dynamic "rotate" 180)
    (cur/get-static "default")))

(defn set-drag-cursor
  "Sets the cursor style for dragging operations.
   
   Args:
     direction - (optional) keyword indicating direction (:ew, :ns, :rotate)"
  [direction]
  (ptk/reify ::set-drag-cursor
    ptk/EffectEvent
    (effect [_ _ _]
      ;; FIXME: this is not honored since  individual components override it
      (set! (.-cursor (.-style (.-body globals/document)))
            (str (cursor-from-direction direction) "!important")))))

(defn- dim-from-direction
  "Given a direction, returns a function that extracts the relevant
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

(defn- set-effect!
  "An effect that sets the state of an atom"
  [^atom flag state]
  (ptk/reify ::set-effect
    ptk/EffectEvent
    (effect [_ _ _]
      (do
        (js/console.log "Numeric input set-effect! flag:" @flag "->" state)
        (reset! flag state)))))


(defn- start-mouse-wheel
  "Starts a mouse wheel event handler to change the value of an input.
   
   Starts an undo transaction limited to (debounced) mouse wheel events.
   After an interval of inactivity, the transaction is committed.

   Args:
     wheel-stream:   a stream of mouse wheel events
     set-delta:      function to call with the new value delta
     direction:      keyword indicating direction to listen to (:ew, :we, :ns, :sn)
     transaction-id: an atom holding a fresh transaction ID to use. It will
                     be reset to nil after the transaction is committed.

   Returns a WatchEvent that listens for mouse wheel events and applies the deltas
   within a transaction,"
  [wheel-stream set-delta direction ^atom transaction-id]
  (let [debounce-ms (get refs/workspace-layout :input-debounce-ms 800)
        get-dim     (dim-from-direction direction)
        set-delta*  (fn [event]
                      (let [delta (get-dim (mse/get-pointer-position (:event event)))
                            up?   (> delta 0)
                            down? (< delta 0)]
                        ;; (js/console.log "Numeric input set-delta*"
                        ;;                 "delta:" delta
                        ;;                 "up?:" up?
                        ;;                 "down?:" down?)
                        (set-delta (:react-event event) up? down? true)))]

    (js/console.log "Numeric input start-mouse-wheel-on-input")

    (ptk/reify ::start-mouse-wheel
      ptk/WatchEvent
      (watch [_ _ stream]
        (let [stopper      (rx/merge
                            (rx/debounce debounce-ms wheel-stream)
                            (mse/drag-stopper stream {:blur? true :up-mouse? false}))
              wheel-stream (rx/take-until stopper wheel-stream)]
          ;; FIXME: potential race conditions:
          ;; - if the user drags the input while the mouse wheel is active,

          (rx/concat
           (rx/of (dwu/start-undo-transaction transaction-id {:timeout false}))
           (rx/tap set-delta* wheel-stream)
           (rx/of (dwu/commit-undo-transaction transaction-id))
           (rx/of (set-effect! transaction-id nil))))))))

(defn on-mouse-wheel* [ref stream set-delta transaction-id]
  (mf/use-callback
   (mf/deps ref stream transaction-id)
   (fn [^js event]
     (when-let [node (mf/ref-val ref)]
       (when (identical? (dom/get-target event) node)
         (let [event      (.getBrowserEvent event)
               event* ^js (nw/normalize-wheel event)
               delta-y    (.-spinY event*)
               delta-x    (.-spinX event*)]
           (dom/prevent-default event)
           (dom/stop-propagation event)
           (dom/select-text! node)
           (js/console.log "Numeric input on-mouse-wheel* " @transaction-id)
           (rx/push! stream {:type :wheel
                             :react-event event
                             :event (mse/->PointerEvent
                                     :delta (gpt/point delta-x delta-y)
                                     (kbd/ctrl? event)
                                     (kbd/shift? event)
                                     (kbd/alt? event)
                                     (kbd/meta? event))})
           (when (nil? @transaction-id)
             (let [id (js/Symbol)]
               (reset! transaction-id id)
               ;; TODO: do I need to make direction configurable e.g. for users of reverse scrolling?
               (st/emit! (start-mouse-wheel stream set-delta :ns transaction-id))))))))))

(defn- start-dragging
  "Starts dragging on an input element to change its value.
   
   Wraps changes in an undo transaction and sets the cursor style.
   
   Args:
     mouse-stream: a stream of mouse events (e.g., pointermove)
     set-delta:    function to call with the new value delta
     direction:    keyword indicating direction (:ew, :we, :ns, :sn, :rotate)
     last-value*:  an atom holding the last value of the input
     transaction-id: an atom holding a fresh transaction ID to use.
                     It will be reset to nil after the transaction is committed.
   
   Returns a WatchEvent that listens for mouse movements and applies the delta."
  [mouse-stream set-delta direction last-value* ^atom transaction-id]
  (js/console.log "Numeric input start-dragging-on-input"
                  "direction:" (name direction))
  (let [sensitivity (get refs/workspace-layout :input-drag-sensitivity 1)  ; pixels per step
        get-dim     (dim-from-direction direction)


        ;; FIXME: this is 
        set-delta*
        (fn [event]
          (let [diff  (get-dim (mse/get-pointer-position (:event event)))
                up?   (> diff 0)
                down? (< diff 0)]
            (when (> (abs diff) sensitivity)
              (set-delta (:react-event event) up? down? true))))

        make-delta-event
        (fn [[prev curr]]
          (let [prev-pos (dom/get-client-position prev)
                curr-pos (dom/get-client-position curr)]
            {:type :delta
             :react-event curr
             :event (mse/->PointerEvent :delta (gpt/subtract curr-pos prev-pos)
                                        (kbd/ctrl? curr)
                                        (kbd/shift? curr)
                                        (kbd/alt? curr)
                                        (kbd/meta? curr))}))]

    (ptk/reify ::start-dragging
      ptk/WatchEvent
      (watch [_ _ stream]
        (let [stopper      (mse/drag-stopper stream)
              delta-stream (->> mouse-stream
                                (pairwise)
                                (rx/take-until stopper)
                                (rx/map make-delta-event))]
          (rx/concat
           (rx/of (set-drag-cursor direction))
           (rx/of (dwu/start-undo-transaction @transaction-id {:timeout false}))
           (rx/tap set-delta* delta-stream)
           (rx/of (dwu/commit-undo-transaction @transaction-id))
           (rx/of (set-drag-cursor nil))
           (rx/of (set-effect! transaction-id nil))
           (rx/of (simple-effect #(js/console.log "last-value* before drag: " @last-value*)))))))))


(defn on-drag-start* [stream set-delta drag-direction last-value* ^atom transaction-id]
  (mf/use-callback
   (mf/deps stream drag-direction last-value* transaction-id)
   (fn [^js event]
     (let [ctrl?      (kbd/ctrl? event)
           shift?     (kbd/shift? event)
           alt?       (kbd/alt? event)
           meta?      (kbd/meta? event)
           pos        (dom/get-client-position event)]
       (js/console.log "Numeric input on-drag-start*"
                       "ctrl?" ctrl? "shift?" shift? "alt?" alt? "meta?" meta?
                       "pos:" pos
                       "drag-direction:" (name drag-direction))
       (dom/prevent-default event)
       (dom/stop-immediate-propagation event)
       ;; TODO: do something adequate here...
       (rx/push! stream {:type :drag
                         :react-event event
                         :event (mse/->PointerEvent :drag pos ctrl? shift? alt? meta?)})
       (when (nil? @transaction-id)
         (let [id (js/Symbol)]
           (reset! transaction-id id)
                      ;; TODO: do I need to make direction configurable e.g. for users of reverse scrolling?
           (st/emit! (start-dragging stream set-delta drag-direction last-value* transaction-id))))))))

(defn parse-value* [ref min-value max-value value nillable? default]
  (mf/use-fn
   (mf/deps min-value max-value value nillable? default)
   (fn []
     (when-let [node (mf/ref-val ref)]
       (let [new-value (-> (dom/get-value node)
                           (str/strip-suffix ".")
                           (smt/expr-eval value))]
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

           :else value))))))

(defn set-delta* [wrap-value? min-value max-value parse-value apply-value default step-value]
  (mf/use-fn
   (mf/deps wrap-value? min-value max-value parse-value apply-value
            default step-value)
   (fn [^js event ^boolean up? ^boolean down? ^boolean transaction?]
     (let [current-value (parse-value)
           current-value
           (cond
             (and (not current-value) down? max-value)
             max-value

             (and (not current-value) up? min-value)
             min-value

             (not current-value)
             (d/nilv default 0)

             :else
             current-value)]
       (when current-value
         (let [increment (cond
                           (kbd/shift? event)
                           (if up? (* step-value 10) (* step-value -10))

                           (kbd/alt? event)
                           (if up? (* step-value 0.1) (* step-value -0.1))

                           :else
                           (if up? step-value (- step-value)))

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
           (apply-value event new-value transaction?)))))))

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
        on-change    (when-let [f (unchecked-get props "onChange")]
                       ;; on-change args: new-value js-event in-transaction?
                       #(if drag-direction (f %1 %2 %3) (f %1 %2)))

        title        (unchecked-get props "title")
        default      (unchecked-get props "default")
        nillable?    (unchecked-get props "nillable")
        class        (d/nilv (unchecked-get props "className") "")
        ;; Whether the tab key should be used to accept the input value before
        ;; moving to the next input.
        tab-accepts? (d/nilv (unchecked-get props "tabAccepts") true)

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

        ;; This `value` represents the previous value and is used as
        ;; initial value for simple math expression evaluation.
        value       (when (not= :multiple value-str) (d/parse-double value-str default))

        ;; Last value before any transactional change
        last-value* (mf/use-var value)

        ;; Current value of the input, updated on-change, used to apply
        ;; unapplied changes upon unmount / blur
        curr-value* (mf/use-var value)


        ;; Whether we are currently in a transaction, e.g. the user is
        ;; using the mouse wheel.
        transaction-id (mf/use-var nil)


        local-stream   (mf/use-memo #(rx/subject))  ;; wheel, kbdup/down, drag events

        update-input
        (mf/use-fn
         (fn [new-value]
           (when-let [node (mf/ref-val ref)]
             (reset! curr-value* new-value)
             (dom/set-value! node (fmt/format-number new-value)))))

        apply-value
        (mf/use-fn
         (mf/deps on-change update-input value)
         (fn [^js event new-value ^boolean transaction?]
           (when (and (not= new-value value)
                      (fn? on-change))
             ;; FIXME: on-change very slow, makes the handler laggy
             (on-change new-value event transaction?))
           (update-input new-value)))

        parse-value (parse-value* ref min-value max-value value nillable? default)
        set-delta (set-delta* wrap-value? min-value max-value parse-value apply-value default step-value)

        on-mouse-wheel (on-mouse-wheel* ref local-stream set-delta transaction-id)
        on-drag-start  (on-drag-start* local-stream set-delta drag-direction last-value* transaction-id)


        ;; handle-on-drag
        ;; (mf/use-fn
        ;;  (mf/deps ref)
        ;;  (fn [event]
        ;;    (when-let [node (mf/ref-val ref)]
        ;;      (when (identical? (dom/get-target event) node)
        ;;        (when (dom/left-mouse? event)
        ;;          (dom/prevent-default event)
        ;;          (dom/stop-immediate-propagation event)
        ;;          (mf/set-ref-val! last-value* (parse-value))
        ;;          (st/emit! (start-dragging set-delta drag-direction last-value*)))))))

        handle-key-down
        (mf/use-fn
         (mf/deps set-delta apply-value update-input parse-value tab-accepts?)
         (fn [^js event]
           (let [up?     (kbd/up-arrow? event)
                 down?   (kbd/down-arrow? event)
                 esc?    (kbd/esc? event)
                 accept? (or (kbd/enter? event) (and (kbd/tab? event) tab-accepts?))
                 node    (mf/ref-val ref)]
             (js/console.log "handle-key-down")
             (cond
               (or down? up?) (let [kbd-event (kbd/->KeyboardEvent
                                               (if up? :up :down)
                                               (.-key event)
                                               (kbd/shift? event)
                                               (kbd/ctrl? event)
                                               (kbd/alt? event)
                                               (kbd/meta? event)
                                               (kbd/mod? event)
                                               (kbd/editing-event? event)
                                               (.-nativeEvent event))]
                                (dom/prevent-default event)
                                (rx/push! local-stream kbd-event))
                                ;; (set-delta kbd-event up? down?)
              ;;  (st/emit! (start-keyboard-action set-delta up? down? last-value*))
               esc?    (do
                         (update-input @last-value*)
                         (dom/blur! node))
               accept? (do
                         (apply-value event @curr-value* false)
                         (reset! last-value* @curr-value*))))))

        ;; TODO: how do I deactivate the default behavior of the input?
        ;; without redefining handle-change? Can I? We don't want to save last-value
        ;; on every change, only before a transaction starts / or after hitting enter
        ;; or blur
        handle-change
        (mf/use-fn
         (mf/deps parse-value value)
         (fn []
           ;; Store the last value inputed 
           (reset! curr-value* (parse-value))
           (js/console.log "handle-change: last=" @last-value* "curr=" @curr-value*)))

        ;; handle-mouse-wheel
        ;; (mf/use-fn
        ;;  (mf/deps set-delta)
        ;;  (fn [event]
        ;;    (when-let [node (mf/ref-val ref)]
        ;;      (js/console.log "handle-mouse-wheel: " @transaction-id)
        ;;      (when (and (dom/active? node) (not @transaction-id))
        ;;        (reset! transaction-id (js/Symbol))  ;; HACK testing
        ;;        (dom/prevent-default event)
        ;;        (dom/stop-propagation event)
        ;;        (reset! last-value* (parse-value))
        ;;        (st/emit! (start-mouse-wheel set-delta :ns node transaction-id))))))

        handle-blur
        (mf/use-fn
         (mf/deps parse-value apply-value update-input on-blur)
         (fn [event]
           (js/console.log "handle-blur")
           (let [new-value @curr-value*
                 old-value (or @last-value* default)]
            ;;  (js/console.log "  last-value: " @last-value*)
             (when (not= new-value old-value)
              ;;  (js/console.log "  new-value: " new-value)
               (if (or nillable? new-value)
                 (apply-value event new-value)
                 (update-input new-value))))
           (when (fn? on-blur)
             (on-blur event))))

        handle-unmount
        (mf/use-callback
         (mf/deps local-stream)
         (fn []
           (js/console.log "handle-unmount")
           (handle-blur)
           (rx/end! local-stream)))  ;; Is this needed?


        ;; on-click
        ;; (mf/use-fn
        ;;  (fn [event]
        ;;    (let [target (dom/get-target event)
        ;;          node   (mf/ref-val ref)]
        ;;      (js/console.log "Numeric input on-click: " target)
        ;;      (when (and (some? node) (not (dom/child? node target)))
        ;;        (dom/blur! node)))))

        handle-focus
        (mf/use-callback
         (mf/deps on-focus select-on-focus?)
         (fn [event]
           (reset! last-value* (parse-value))
           (let [target (dom/get-target event)]
             (when on-focus
               (on-focus event))

             (when select-on-focus?
               (dom/select-text! target)
               ;; In webkit browsers the mouseup event will be called after the on-focus causing and unselect
               (.addEventListener target "mouseup" dom/prevent-default #js {:once true})))))

        props (-> (obj/clone props)
                  (obj/unset! "dragDirection")
                  (obj/unset! "selectOnFocus")
                  (obj/unset! "nillable")
                  (obj/set! "value" mf/undefined)
                  (obj/set! "onChange" handle-change)
                  (obj/set! "className" (str/join " " [class (cursor-from-direction drag-direction)]))
                  (obj/set! "type" "text")
                  (obj/set! "ref" ref)
                  (obj/set! "defaultValue" (fmt/format-number value))
                  (obj/set! "title" title)
                  (obj/set! "onKeyDown" handle-key-down)
                  (obj/set! "onDragStart" on-drag-start)
                  (obj/set! "onBlur" handle-blur)
                  (obj/set! "onFocus" handle-focus))]

    ;; (mf/with-effect [local-stream ref]
    ;;   (when-let [node (mf/ref-val ref)]
    ;;     (let [debounce-ms (get refs/workspace-layout :input-debounce-ms 800)
    ;;           get-dim     (dim-from-direction :ns)  ;; TODO: need to allow reversing for OSX here?
    ;;           set-delta*  (fn [local-event]
    ;;                         (let [event (:react-event local-event)
    ;;                               delta (mse/get-pointer-position (:event local-event))
    ;;                               delta (get-dim delta)
    ;;                               up?   (> delta 0)
    ;;                               down? (< delta 0)]
    ;;                           (js/console.log "Numeric input set-delta*"
    ;;                                           "delta:" delta
    ;;                                           "up?:" up?
    ;;                                           "down?:" down?)
    ;;                           (set-delta event up? down? true)))
    ;;           wheel-stream (rx/filter #(= :wheel (:type %)) local-stream)
    ;;           stopper      (rx/merge
    ;;                         (rx/debounce debounce-ms wheel-stream)
    ;;                         (mse/drag-stopper local-stream {:blur? false :up-mouse? false}))
    ;;           undo-id     (js/Symbol)]
    ;;       (rx/sub! (->> wheel-stream
    ;;                     (rx/take-until stopper)
    ;;                     (rx/tap #(dom/select-text! node)))
    ;;                set-delta*))))

    (mf/with-effect [value]
      (when-let [input-node (mf/ref-val ref)]
        (dom/set-value! input-node (fmt/format-number value))))

    (mf/with-effect [handle-unmount] handle-unmount)

    ;; (mf/with-layout-effect []
    ;;   (let [keys [(events/listen globals/window "pointerdown" on-click)
    ;;               (events/listen globals/window "click" on-click)]]
    ;;     #(run! events/unlistenByKey keys)))

    (mf/with-effect []
      (when-let [node (mf/ref-val ref)]
        (let [key (events/listen node "wheel" on-mouse-wheel #js {:passive false})]
          #(events/unlistenByKey key))))

    [:> :input props]))