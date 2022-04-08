package com.twitter.finagle.thrift.filter

import com.twitter.conversions.DurationOps.richDurationFromInt
import com.twitter.finagle.Service
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.tracing.Annotation.BinaryAnnotation
import com.twitter.finagle.tracing.BufferingTracer
import com.twitter.finagle.tracing.Record
import com.twitter.finagle.tracing.Trace
import com.twitter.scrooge.thrift_validation.ThriftValidationException
import com.twitter.util.Await
import com.twitter.util.Future
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

class ValidationReportingFilterTest extends AnyFunSuite with MockitoSugar with Matchers {

  def await[T](f: Future[T]): T = Await.result(f, 5.seconds)
  val request = Array.empty[Byte]
  val exception = new ThriftValidationException("endpoint", classOf[String], Set.empty)

  val exceptionService = new Service[Array[Byte], Array[Byte]] {
    def apply(request: Array[Byte]): Future[Array[Byte]] = {
      throw exception
    }
  }

  // a mock service with no exception thrown
  val service = new Service[Array[Byte], Array[Byte]] {
    def apply(request: Array[Byte]): Future[Array[Byte]] = {
      Future.value(request)
    }
  }

  test("services that throw TVE populated correctly in the statsReceiver") {
    val receiver = new InMemoryStatsReceiver
    val filteredService = new ValidationReportingFilter(receiver).andThen(exceptionService)

    intercept[ThriftValidationException] {
      await(filteredService(request))
    }

    assert(
      receiver.counters(Seq("violation", exception.endpoint, exception.requestClazz.getName)) == 1)
  }

  test("services with no exception thrown are populated correctly in the statsReceiver") {
    val receiver = new InMemoryStatsReceiver
    val filteredService = new ValidationReportingFilter(receiver).andThen(service)

    await(filteredService(request))
    assert(!receiver.counters.toString().startsWith("violation"))
  }

  test("check validation spans are annotated with right identifier") {
    val receiver = new InMemoryStatsReceiver
    val filteredService = new ValidationReportingFilter(receiver).andThen(exceptionService)
    val traceId = Trace.id
    val tracer = new BufferingTracer
    Trace.letTracerAndId(tracer, traceId) {
      intercept[ThriftValidationException] {
        await(filteredService(request))
      }
    }

    def getAnnotation(tracer: BufferingTracer, name: String): Option[Record] = {
      tracer.toSeq.find { record =>
        record.annotation match {
          case a: BinaryAnnotation if a.key == name => true
          case _ => false
        }
      }
    }

    assert(getAnnotation(tracer, "validation/request").isDefined)
    assert(getAnnotation(tracer, "validation/endpoint").isDefined)
  }
}
