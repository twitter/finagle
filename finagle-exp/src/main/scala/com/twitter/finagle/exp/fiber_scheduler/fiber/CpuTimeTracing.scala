package com.twitter.finagle.exp.fiber_scheduler.fiber

import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.tracing.TraceId
import com.twitter.logging.Logger

/**
 * Cpu time tracing is done in three steps to accommodate
 * the needs of the different fibers. The generated annotations
 * have the cpu time information in nanoseconds.
 *
 * One `CpuTimeTracing` is created for each type of fiber.
 * Each instance bounds the fiber type key string and the
 * percentage of traced requests.
 *
 * `CpuTimeTracing.Start` is bounded to a `Trace` if it's
 * enabled and can be reused for multiple measurements
 * under the same trace.
 *
 * `CpuTimeTracing.Stop` is bounded to an individual
 * cpu time measurement and produces the trace annotation
 * when applied.
 */
private[fiber] final class CpuTimeTracing(tpe: String, percentage: Int) {

  import CpuTimeTracing._

  /**
   * This dynamic annotation key has low cardinality because
   * it's used by the three different types of fibers, which
   * is something fundamental to the design of the scheduler
   * and is unlikely to change.
   */
  private[this] val key = s"srv/fiber/cpu_time_ns/$tpe"

  private[this] val enabled =
    Trace.enabled &&
      threadMxBean.isCurrentThreadCpuTimeSupported &&
      percentage > 0

  /**
   * Lightweight mechanism to enable cpu time tracking for a
   * percentage of the trace requests via a bit mask to avoid
   * a mod operation.
   */
  private[this] val shouldTrace: TraceId => Boolean = {
    val percentageMask = 127
    val percentageMark =
      (percentageMask * percentage) / 100
    (id: TraceId) => (id.hashCode() & percentageMask) < percentageMark
  }

  def apply(): CpuTimeTracing.Start = {
    if (enabled) {
      val trace = Trace()
      val traceId = trace.idOption
      if (trace.isActivelyTracing && traceId.exists(shouldTrace)) { () =>
        val start = threadMxBean.getCurrentThreadCpuTime
        () => {
          val time = threadMxBean.getCurrentThreadCpuTime - start
          log.debug("%s: %s (%s)", key, time, traceId)
          trace.recordBinary(key, time)
        }
      } else {
        nullStart
      }
    } else {
      nullStart
    }
  }
}

private[fiber] final object CpuTimeTracing {

  abstract class Start {
    def apply(): Stop
  }
  abstract class Stop {
    def apply(): Unit
  }

  private val log = Logger()

  private val threadMxBean =
    java.lang.management.ManagementFactory.getThreadMXBean()

  final val nullStart: Start = {
    val nullStop: Stop = () => {}
    () => nullStop
  }
}
