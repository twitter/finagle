package com.twitter.finagle.logging

import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.util.WindowedPercentileHistogram
import com.twitter.finagle.Filter
import com.twitter.finagle.Service
import com.twitter.finagle.SimpleFilter
import com.twitter.util.Future
import com.twitter.util.Stopwatch
import com.twitter.util.Timer
import com.twitter.util.logging.Logger
import org.slf4j.LoggerFactory

/**
 * Logs trace ids that are slower than `percentile` of other requests.
 *
 * Note that because this only logs very slow traces that are also sampled, during normal
 * operation you should expect (1 - percentile) * sampleRate requests to be loggged.
 *
 * So if your sample rate is 1/10,000 and you're looking for p99 latency, you will expect
 * to see 1 out of every 1,000,000 requests sampled.  However, when your service is overloaded,
 * you will see a much higher proportion of requests sampled.
 *
 * @percentile the percentile latency above which we log debug data, in the range [0.0, 1.0]
 * @logFn the function for logging the debug data, used for setting the desired log level
 */
class SlowTracesFilter private[logging] (
  percentile: Double,
  logFn: String => Unit,
  timer: Timer)
    extends Filter.TypeAgnostic {

  // we use this indirection to get around Scala's restrictive constructor
  // argument guidelines
  private[this] def this(percentile: Double, log: Logger) =
    this(percentile, log.info(_), DefaultTimer)

  // we can't use getClass because it doesn't compile in 2.13.1
  def this(percentile: Double) =
    this(percentile, Logger(LoggerFactory.getLogger(classOf[SlowTracesFilter])))

  def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] = new SimpleFilter[Req, Rep] {
    private[this] val latencies = new WindowedPercentileHistogram(timer)
    def apply(req: Req, svc: Service[Req, Rep]): Future[Rep] = {
      val elapsed = Stopwatch.start()
      svc(req).ensure {
        val latency = elapsed()
        val millis = latency.inMilliseconds.toInt
        latencies.add(millis)
        if (Trace.isActivelyTracing) {
          val percValue = latencies.percentile(percentile)
          if (percValue >= WindowedPercentileHistogram.DefaultLowestDiscernibleValue &&
            percValue < millis) {
            logFn(
              s"Detected very slow request that's sampled by tracing. $percentile latency ${percValue}ms, measured request latency ${millis}ms, Trace ID ${Trace.id.traceId}")
          }
        }
      }
    }
  }
}
