package com.twitter.finagle.tracing

import com.twitter.finagle.tracing.Annotation.Message
import com.twitter.logging.{Level, LogRecord}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TracingLogHandlerTest extends FunSuite with BeforeAndAfter {
  test("TracingLogHandler: send messages to Tracer") {
    val tracer = new BufferingTracer()
    Trace.letTracer(tracer) {
      val handler = new TracingLogHandler
      val msg1 = "hello"
      handler.publish(new LogRecord(Level.DEBUG, msg1))
  
      tracer.iterator.next().annotation match {
        case Message(s) => assert(s.trim == msg1)
        case _ => fail("Message does not match")
      }
    }
  }
}
