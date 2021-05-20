package com.twitter.finagle.tracing

import com.twitter.conversions.DurationOps.RichDuration
import com.twitter.finagle.client.utils.StringClient
import com.twitter.finagle.server.utils.StringServer
import com.twitter.finagle.tracing.Annotation.BinaryAnnotation
import com.twitter.finagle.{Address, Name, Service}
import com.twitter.util.{Await, Future, Time}
import java.net.InetSocketAddress
import org.scalatest.funsuite.AnyFunSuite

class ForwardAnnotationTest extends AnyFunSuite {
  test("ChildTraceContext.let traces in child context") {
    def getAnnotation(tracer: BufferingTracer, name: String): Option[Record] = {
      tracer.toSeq.find { record =>
        record.annotation match {
          case a: BinaryAnnotation if a.key == name => true
          case _ => false
        }
      }
    }

    Time.withCurrentTimeFrozen { _ =>
      val svc = new Service[String, String] {
        def apply(request: String): Future[String] = {
          Future(request)
        }
      }

      val echoServer = StringServer.server.serve(new InetSocketAddress(0), svc)

      val tracer = new BufferingTracer()

      val client = StringClient.client
        .withTracer(tracer)

      val service = client.newService(
        Name.bound(
          Address(echoServer.boundAddress
            .asInstanceOf[InetSocketAddress])),
        "strings")

      Trace.letTracer(tracer) {
        Trace.record(BinaryAnnotation("test_parent", true))
        val parentTraceId = Trace.id
        val result = ForwardAnnotation.let(BinaryAnnotation("test_child", true)) {
          service("hi")
        }

        Await.result(result, 5.seconds)

        assert(getAnnotation(tracer, "test_parent").get.traceId == parentTraceId)
        assert(getAnnotation(tracer, "test_child").get.traceId != parentTraceId)
      }
    }
  }
}
