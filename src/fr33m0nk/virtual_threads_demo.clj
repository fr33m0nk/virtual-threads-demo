(ns fr33m0nk.virtual-threads-demo
  (:require
    [clojure.core.async.impl.protocols :as protocols]
    [clojure.core.async.impl.dispatch :as dispatch]
    [clojure.core.async :as a]
    [hato.client :as http])
  (:import (java.util.concurrent CyclicBarrier
                                 Executors
                                 StructuredTaskScope
                                 StructuredTaskScope$ShutdownOnFailure
                                 StructuredTaskScope$ShutdownOnSuccess
                                 StructuredTaskScope$Subtask
                                 StructuredTaskScope$Subtask$State)
           (java.util.function Function)))

;; Virtual threads are a part of JDK 19+
;; Virtual thread is a JDK construct similar to Coroutines in Kotlin and Go routines in Go
;; JDK schedules Virtual threads on Platform threads using m:n scheduling
;; Virtual threads are scheduled on special ForkJoin worker thread pools


;; Why virtual threads?
;; Virtual are very lightweight is comparison to platform threads
;; Whenever JDK detects a blocking operation:
;; - it parks the blocking virtual threads (by unmounting virtual thread from platform thread)
;; - allows other virtual thread to carry on execution
;; - blocking virtual threads is a cheap operation
;; Earlier craft:
;; - asynchronous programming e.g. CompletableFuture
;; - code rewrite e.g. core.async
;; - Java agent based runtime instrumentation via bytecode rewrite e.g. kilim, quasar

;; Spinning up a 100,000 virtual threads
(comment

  ;;platform threads
  ;; Below code blows up REPL!!
  (let [counter (atom 0)
        barrier (CyclicBarrier. 100000)
        platform-threads (into []
                              (map (fn [i]
                                     (-> (Thread/ofPlatform)
                                         (.unstarted #(try
                                                        (Thread/sleep (long (rand-int 2000)))
                                                        (.await barrier)
                                                        (catch Exception e
                                                          (throw e)))))))
                              (range 100000))]

    (doall (pmap #(do (.start %) (swap! counter inc)) platform-threads))
    (doall (pmap #(.join %) platform-threads))
    (println @counter))

  ;;virtual threads

  (let [counter (atom 0)
        barrier (CyclicBarrier. 500000)
        virtual-threads (into []
                              (map (fn [i]
                                     (-> (Thread/ofVirtual)
                                         (.unstarted #(try
                                                        (Thread/sleep (long (rand-int 2000)))
                                                        (.await barrier)
                                                        (catch Exception e
                                                          (throw e)))))))
                              (range 500000))]

    (doall (pmap #(do (.start %) (swap! counter inc)) virtual-threads))
    (doall (pmap #(.join %) virtual-threads))
    (println @counter)))

;; Following function takes some time doing IO
(defn make-api-request
  []
  (http/get "https://httpbin.org/get"))

;; How to use virtual threads?
;; 1
(defn start-virtual-thread
  [f]
  (Thread/startVirtualThread f))

(comment

  (let [p (promise)
        vt (start-virtual-thread #(deliver p (select-keys (make-api-request) [:status])))]
    (.join vt)
    @p))

;; 2
;; JDK ships with a special executor which launch a new virtual thread per task
;; This can be used in place of any Executor service created for IO tasks
(def virtual-thread-executor (Executors/newVirtualThreadPerTaskExecutor))
(comment
  @(.submit virtual-thread-executor #(+ 1 2))
  )

;; replacing core.async's executor service
;; This does not work with a/pipeline operators or a/thread!! as they use different thread pool

;; Rick and Morty example
;; Problem: get character profiles for first two episodes of Rick and Morty
;; Different endpoints for characters per episode and individual character profile
(comment

  (def vt-core-async-executor
    (-> (Thread/ofVirtual)
        (.name "core-async-" 0)
        (.factory)
        (Executors/newThreadPerTaskExecutor)))

  (defonce async-virtual-executor
           (reify protocols/Executor
             (protocols/exec [_ r]
               (.execute vt-core-async-executor ^Runnable r))))

  (alter-var-root #'dispatch/executor
                  (constantly (delay async-virtual-executor)))

  (let [episodes [1 2]
        get-request #(http/get % {:as :json})
        rick-morty-episode-cast-api (fn [episode]
                                      (get-request (str "https://rickandmortyapi.com/api/episode/" episode)))

        episode-cast-chan (->> (mapv #(a/go
                                        (println "Episode cast thread : " (Thread/currentThread))
                                        (-> (rick-morty-episode-cast-api %) :body :characters)) episodes)
                               (a/merge)
                               (a/transduce (comp (mapcat identity) (distinct)) conj #{}))
        character-chan (->> (a/<!! episode-cast-chan)
                            (mapv #(a/go (-> (get-request %) :body)))
                            (a/merge)
                            (a/into []))]

    (a/<!! character-chan)))

;; 3
;; Structured Task scope (Incubator feature in JDK 19 and 20. Preview feature in JDK 21)
;; Structured concurrency emphasises on writing imperative code and blocking virtual threads as and when needed
;; Structured task scope acts as a supervisor for all the subtasks
;; All subtasks run in their own virtual thread

;; 3.1 StructuredTaskScope class can be used directly
;; the default behaviour can be overridden to support a use case
(defn ->structured-scope
  ^StructuredTaskScope
  [success-handler error-handler]
  (proxy [StructuredTaskScope] []
    (handleComplete [^StructuredTaskScope$Subtask subtask]
      (condp = (.state subtask)
        StructuredTaskScope$Subtask$State/UNAVAILABLE (throw (IllegalArgumentException. "SubTask is unavailable"))
        StructuredTaskScope$Subtask$State/FAILED (error-handler (.exception subtask))
        StructuredTaskScope$Subtask$State/SUCCESS (success-handler (.get subtask))))))

(comment

  (let [accumulators (repeatedly 2 #(atom []))              ;; create success and error accumulators
        [success-handler error-handler] (map (fn [acc] (partial swap! acc conj)) accumulators)]
    (with-open [scope (->structured-scope success-handler error-handler)]
      (let [api-request (.fork scope #(-> (make-api-request) (select-keys [:status])))
            failure (.fork scope #(throw (ex-info "boom" {})))
            some-slow-task (.fork scope #(do (Thread/sleep 5000) :some-slow-task))]

        ; Status of tasks before join: UNAVAILABLE
        (run! #(printf "\nStatus of tasks before join: %s\n" (.state %)) [api-request failure some-slow-task])
        (.join scope)

        ; Status of tasks after join: Success
        (run! #(printf "\nStatus of tasks after join: %s\n" (.state %)) [api-request failure some-slow-task])
        (->> accumulators
             (map deref)
             (zipmap [:success :failure]))))))

;; Two customizations of StructuredTaskScope are offered out of the box
;; 3.2 Shutdown On success
(comment

  (with-open [scope (StructuredTaskScope$ShutdownOnSuccess.)]
    (let [some-slow-task (.fork scope #(do (println "Sleeping")
                                           (Thread/sleep 5000)
                                           (println "Awake")
                                           :some-slow-task))
          api-request (.fork scope #(-> (make-api-request) (select-keys [:status])))]
      (.join scope)
      (.result scope))))

;; 3.3 Shutdown On Failure
(comment

  (with-open [scope (StructuredTaskScope$ShutdownOnFailure.)]
    (let [some-slow-task (.fork scope #(do (println "Sleeping")
                                           (Thread/sleep 5000)
                                           (println "Awake")
                                           :some-slow-task))
          failure (.fork scope #(throw (ex-info "boom" {})))
          api-request (.fork scope #(-> (make-api-request) (select-keys [:status])))]
      (.join scope)
      (.throwIfFailed scope)

      [(.get some-slow-task)
       (.get api-request)])))

;;; Gotchas
;; Virtual thread would be pinned to Platform thread if:
;; code uses synchronized blocks
;; code does FFI e.g. native code interop using JNI

;;; Platform vs Virtual thread?
;; Use platform threads when doing non-blocking and computationally intensive work
;; Use virtual threads when doing I/O intensive or computationally inexpensive work


;; Rick and Morty example
;; Problem: get character profiles for first two episodes of Rick and Morty
;; Different endpoints for characters per episode and individual character profile

(comment
  (let [get-request #(http/get % {:as :json})
        ;get-request (fn [_] (throw (Exception. "BOOM")))
        custom-exception-handler (fn [scope]
                                   (reify
                                     Function
                                     (apply [_ ex]
                                       (ex-info (str "My exception " (.getMessage ex))
                                                {:error-api scope
                                                 :stacktrace (.getStackTrace ex)}))))]
    (with-open [episode-scope (StructuredTaskScope$ShutdownOnFailure.)]
      (let [episodes [1 2]
            rick-morty-episode-cast-api (fn [episode]
                                          (get-request (str "https://rickandmortyapi.com/api/episode/" episode)))
            ;; forking tasks inside functions that produce lazy sequence will lead to exceptions
            episode-tasks (mapv (fn [episode] (.fork episode-scope #(rick-morty-episode-cast-api episode))) episodes)]
        (.join episode-scope)

        ;; Also takes a custom exception handler
        (.throwIfFailed episode-scope (custom-exception-handler :episode-scope))

        (let [character-uris (into #{} (mapcat #(-> % .get :body :characters)) episode-tasks)]
          (with-open [character-scope (StructuredTaskScope$ShutdownOnFailure.)]
            (let [characters (mapv
                               (fn [uri] (.fork character-scope #(get-request uri)))
                               character-uris)]
              (.join character-scope)
              (.throwIfFailed character-scope (custom-exception-handler :character-scope))

              (into [] (map #(-> % .get :body)) characters))))))))


;; The number of platform threads for worker pool is controlled by below parameters:
;; `-Djdk.virtualThreadScheduler.parallelism=1`
;; `-Djdk.virtualThreadScheduler.maxPoolSize=1`
;; `-Djdk.virtualThreadScheduler.minRunnable=1`
