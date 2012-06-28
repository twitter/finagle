package com.twitter.finagle.tracing

import com.twitter.finagle.tracing.Annotation.Message
import com.twitter.logging.{Level, LogRecord}
import org.specs.SpecificationWithJUnit

class TracingLogHandlerSpec extends SpecificationWithJUnit {
  "TracingLogHandler" should {
    doBefore { Trace.clear() }

    "send messages to Tracer" in {
      val tracer = new BufferingTracer()
      Trace.pushTracer(tracer)

      val handler = new TracingLogHandler
      val msg1 = "hello"
      handler.publish(new LogRecord(Level.DEBUG, msg1))

      tracer.iterator.next().annotation match {
        case Message(s) => s.trim must_== msg1
        case _ => fail("Message does not match")
      }
    }
  }
}
