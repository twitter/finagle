package com.twitter.finagle.offload

import com.twitter.finagle.stats.StatsReceiver
import java.util.concurrent.ExecutorService

/**
 * Experimental factory for generating the `ExecutorService` used to offload work from the
 * Netty threads.
 *
 * This is available so custom pools can be created, potentially with more interesting behavior
 * and/or telemetry.
 */
abstract class OffloadThreadPoolFactory {

  /** Construct a new `ExecutorService`
   *
   * @param poolSize The size of the pool as configured by the finagle flags
   *                 `com.twitter.finagle.offload.numWorkers` and `com.twitter.finagle.offload.auto`
   * @param stats `StatsReceiver` to use for observability.
   */

  def newPool(poolSize: Int, stats: StatsReceiver): ExecutorService

  /** Implementors should make the `toString` method meaningful and it will be used in log entries */
  override def toString: String
}
