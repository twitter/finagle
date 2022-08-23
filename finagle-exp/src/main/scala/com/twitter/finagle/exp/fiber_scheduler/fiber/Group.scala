package com.twitter.finagle.exp.fiber_scheduler.fiber

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicInteger

/**
 * Groups represent the scheduling group of `ForkedFiber`s.
 * Each `ForkedFiber` has a `Group` and different `Group`
 * implementations can be used customize the scheduling
 * behavior of their fibers.
 */
private[fiber_scheduler] abstract class Group {

  /**
   * Tries to schedule a fiber (called by `FiberScheduler`)
   * @param fiber the fiber to be scheduled
   * @return true if successful, false otherwise
   */
  def trySchedule(fiber: ForkedFiber): Boolean

  /**
   * Schedules a fiber with a bias to using the
   * `bias` worker. This method forces scheduling
   * and must not fail (called by `FiberScheduler`)
   * @param fiber the fiber to be scheduled
   * @param bias the worker that should be tried first
   */
  def schedule(fiber: ForkedFiber): Unit

  /**
   * Re-activates a fiber that was in a suspended state
   * by re-scheduling it (called by `FiberScheduler` and
   * `ForkedFiber`)
   * @param fiber a suspended fiber that is activating again
   */
  def activate(fiber: ForkedFiber): Unit

  /**
   * Indicates that the fiber doesn't have more tasks to execute
   * and is entering a suspended state. If the fiber is activated
   * later, it goes through the regular scheduling flow again.
   * (called by `ForkedFiber`)
   * @param fiber the fiber that is being suspended
   */
  def suspend(fiber: ForkedFiber): Unit
}

private[fiber_scheduler] object Group {

  /**
   * A default group that just redirects the scheduling
   * calls to the provided functions.
   */
  def default(
    _trySchedule: ForkedFiber => Boolean,
    _schedule: ForkedFiber => Unit
  ): Group =
    new Group {
      override def trySchedule(fiber: ForkedFiber): Boolean = _trySchedule(fiber)
      override def schedule(fiber: ForkedFiber): Unit = _schedule(fiber)
      override def activate(fiber: ForkedFiber): Unit = _schedule(fiber)
      override def suspend(fiber: ForkedFiber): Unit = {}
    }

  /**
   * A group that limits the synchronous concurrency of the execution
   * of its fibers. The implementation is similar to `AsyncSemaphore`
   * but optimized to the needs of the fiber scheduler.
   */
  def withMaxSyncConcurrency(
    concurrency: Int,
    maxWaiters: Int,
    _trySchedule: ForkedFiber => Boolean,
    _schedule: ForkedFiber => Unit
  ): Group =
    new Group {

      // if >= 0, # of permits
      // if < 0, # of waiters
      private[this] val state = new AtomicInteger(concurrency)
      private[this] val waiters = new ConcurrentLinkedQueue[ForkedFiber]()

      override def trySchedule(fiber: ForkedFiber): Boolean = {
        def loop(): Boolean = {
          val s = state.get()
          if (s > 0 && state.compareAndSet(s, s - 1)) {
            // permit acquired successfully
            val ret = _trySchedule(fiber)
            if (!ret) {
              // roll back state change. It's important to call `suspend`
              // instead of just rolling back the state since a new waiter
              // could be added by another thread while this thread was
              // trying to schedule the fiber.
              suspend(fiber)
            }
            ret
          } else if (maxWaiters != Int.MaxValue && -s == maxWaiters) {
            // reached max waiters
            false
          } else if (s <= 0 && state.compareAndSet(s, s - 1)) {
            // failed to acquire permit, add waiter
            waiters.add(fiber)
            true
          } else {
            // CAS operation failed, retry
            loop()
          }
        }
        loop()
      }

      override def schedule(fiber: ForkedFiber): Unit = {
        def loop(): Unit = {
          val s = state.get()
          if (s > 0 && state.compareAndSet(s, s - 1)) {
            // permit acquired successfully
            _schedule(fiber)
          } else if (maxWaiters != Int.MaxValue && -s == maxWaiters) {
            // reached max waiters
            throw new RejectedExecutionException("fiber group reached its max waiters limit")
          } else if (s <= 0 && state.compareAndSet(s, s - 1)) {
            // failed to acquire permit, add waiter
            waiters.add(fiber)
          } else {
            // CAS operation failed, retry
            loop()
          }
        }
        loop()
      }

      override def activate(fiber: ForkedFiber): Unit = {
        // no need to update the state, just reuse
        // the previously acquired permit
        _schedule(fiber)
      }

      override def suspend(fiber: ForkedFiber): Unit = {
        val s = state.getAndIncrement()
        if (s < 0) {
          var w = waiters.poll()
          // accounts for the race condition in `schedule` and `trySchedule`
          // when the state is updated to indicate that a waiter is being added
          // but the waiter hasn't been added to the waiters queue yet. As in
          // `AsyncSemaphore`, it's safe to retry until it succeeds since the
          // thread that indicated that a waiter will be added can only proceed
          // to adding the waiter immediately after the state change.
          while (w == null) {
            w = waiters.poll()
          }
          _schedule(w)
        }
      }
    }
}
