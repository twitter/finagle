package com.twitter.finagle.filter

import com.twitter.util.{Time, Future}
import com.twitter.jvm.Jvm
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.tracing.{Trace, Record, Annotation}
import com.twitter.conversions.DurationOps._

private object MkJvmFilter {

  private val GcStartAnnotation = Annotation.Message("GC Start")

  private val GcEndAnnotation = Annotation.Message("GC End")

}

/**
 * Given a `com.twitter.util.Jvm`, use [[apply]] to create a `Filter`
 * to trace garbage collection events.
 */
class MkJvmFilter(jvm: Jvm) {
  import MkJvmFilter._

  private[this] val recentGcs = jvm.monitorGcs(1.minute)

  def apply[Req, Rep](): SimpleFilter[Req, Rep] = new SimpleFilter[Req, Rep] {
    def apply(req: Req, service: Service[Req, Rep]): Future[Rep] = {
      val trace = Trace()
      if (!trace.isActivelyTracing) {
        service(req)
      } else {
        val begin = Time.now
        service(req).ensure {
          val gcs = recentGcs(begin)
          var totalMs = 0L
          gcs.foreach { gc =>
            totalMs += gc.duration.inMilliseconds
            trace.record(Record(trace.id, gc.timestamp, GcStartAnnotation, None))
            trace.record(Record(trace.id, gc.timestamp + gc.duration, GcEndAnnotation, None))
          }
          trace.recordBinary("jvm/gc_count", gcs.size)
          trace.recordBinary("jvm/gc_ms", totalMs)
        }
      }
    }
  }
}
