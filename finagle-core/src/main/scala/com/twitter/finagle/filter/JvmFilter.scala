package com.twitter.finagle.filter

import com.twitter.util.{Time, Future}
import com.twitter.jvm.Jvm
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.tracing.{Trace, Record, Annotation}
import com.twitter.conversions.time._

/**
 * Given a Jvm, create filters to record GCs (and other JVM events in
 * the future).
 */
class MkJvmFilter(jvm: Jvm) {
  private[this] val buffer = jvm.monitorGcs(1.minute)

  def apply[Req, Rep](): SimpleFilter[Req, Rep] = new SimpleFilter[Req, Rep] {
    def apply(req: Req, service: Service[Req, Rep]): Future[Rep] = {
      val begin = Time.now
      if (Trace.isActivelyTracing) {
        service(req) ensure {
          buffer(begin) foreach { gc =>
            Trace.record {
              Record(Trace.id, gc.timestamp, Annotation.Message(gc.toString), Some(gc.duration))
            }
          }
        }
      }
      else
        service(req)
    }
  }
}

