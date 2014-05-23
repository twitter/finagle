package com.twitter.finagle.tracing

import com.twitter.finagle.Service
import com.twitter.util.{Await, Future}
import org.junit.runner.RunWith
import org.mockito.ArgumentCaptor
import org.mockito.Matchers.any
import org.mockito.Mockito.{spy, verify, when, atLeastOnce}
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSuite}
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class TracingFilterTest extends FunSuite with MockitoSugar with BeforeAndAfter {
  before { Trace.clear() }

  val service = mock[Service[Int, Int]]
  when(service(any[Int])).thenReturn(Future.value(4))

  val tracer = spy(new NullTracer)
  val filter = new TracingFilter[Int, Int](tracer, "tracerTest")

  test("TracingFilter: should trace Finagle version") {
    val captor = ArgumentCaptor.forClass(classOf[Record])
    val composed = filter andThen service

    Await.result(composed(4))
    verify(tracer, atLeastOnce()).record(captor.capture())

    val versionKeyFound = captor.getAllValues.asScala.exists { record =>
      record.annotation match {
        case Annotation.BinaryAnnotation(key, _) => key == "finagle.version"
        case _ => false
      }
    }

    assert(versionKeyFound, "Finagle version wasn't traced as a binary record")
  }
}
