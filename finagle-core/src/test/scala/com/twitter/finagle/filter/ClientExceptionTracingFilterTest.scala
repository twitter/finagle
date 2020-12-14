package com.twitter.finagle.filter

import com.twitter.conversions.DurationOps.RichDuration
import com.twitter.finagle.stack.nilStack
import com.twitter.finagle.tracing.Annotation.BinaryAnnotation
import com.twitter.finagle.tracing.{BufferingTracer, Record, Trace}
import com.twitter.finagle.{Service, ServiceFactory, Stack, StackBuilder}
import com.twitter.util.{Await, Future}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

class ClientExceptionTracingFilterTest extends AnyFunSuite with MockitoSugar {

  def tracingAnnotations(tracer: BufferingTracer): Seq[(String, Any)] = {
    tracer.iterator.toList collect {
      case Record(_, _, BinaryAnnotation(k, v), _) => k -> v
    }
  }

  test("error annotation is false when the service succeeds") {
    val svcModule = new Stack.Module0[ServiceFactory[String, String]] {
      val role = Stack.Role("svcModule")
      val description = ""
      def make(next: ServiceFactory[String, String]) =
        ServiceFactory.const(new Service[String, String] {
          def apply(request: String): Future[String] = {
            Future(request)
          }
        })
    }
    val tracer = new BufferingTracer()
    Trace.letTracer(tracer) {
      val factory =
        new StackBuilder[ServiceFactory[String, String]](nilStack[String, String])
          .push(svcModule)
          .push(ClientExceptionTracingFilter.module())
          .make(Stack.Params.empty)

      val svc = Await.result(factory(), 2.seconds)
      Await.result(svc(("hi")), 2.seconds)

      val expected = Seq()
      assert(tracingAnnotations(tracer) == expected)
    }
  }

  test("exceptions are annotated when thrown") {
    val svcModule = new Stack.Module0[ServiceFactory[String, String]] {
      val role = Stack.Role("svcModule")
      val description = ""
      def make(next: ServiceFactory[String, String]) =
        ServiceFactory.const(Service.mk[String, String] { str: String =>
          Future.exception(new IllegalArgumentException("Exc Message"))
        })
    }
    val tracer = new BufferingTracer()
    Trace.letTracer(tracer) {
      val factory =
        new StackBuilder[ServiceFactory[String, String]](nilStack[String, String])
          .push(svcModule)
          .push(ClientExceptionTracingFilter.module())
          .make(Stack.Params.empty)

      val svc = Await.result(factory(), 2.seconds)
      svc(("start"))

      val expected = Seq(
        ("error", true),
        ("exception.type", "java.lang.IllegalArgumentException"),
        ("exception.message", "Exc Message"))
      assert(tracingAnnotations(tracer) == expected)
    }
  }
}
