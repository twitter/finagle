package com.twitter.finagle.builder

import com.twitter.finagle.Failure
import com.twitter.finagle.RequestException
import java.io.IOException
import java.util.logging.Level
import java.util.logging.Logger
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.{eq => mockitoEq}
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class SourceTrackingMonitorTest extends AnyFunSuite with MockitoSugar {
  test("handles unrolling properly") {
    val logger = mock[Logger]
    val monitor = new SourceTrackingMonitor(logger, "qux")
    val e = new Exception
    val f1 = new Failure("foo", Some(e), sources = Map(Failure.Source.Service -> "tweet"))
    val f2 = new Failure("bar", Some(f1))
    val exc = new RequestException(f2)
    exc.serviceName = "user"
    monitor.handle(exc)
    verify(logger).log(
      Level.SEVERE,
      "The 'qux' service " +
        Seq("user", "tweet").mkString(" on behalf of ") +
        " threw an exception",
      exc
    )
  }

  test("logs IOExceptions at Level.FINE") {
    val logger = mock[Logger]
    val ioEx = new IOException("hi")
    val monitor = new SourceTrackingMonitor(logger, "umm")
    monitor.handle(ioEx)
    verify(logger).log(mockitoEq(Level.FINE), any(), mockitoEq(ioEx))
  }

  test("logs Failure.rejected at Level.FINE") {
    val logger = mock[Logger]
    val monitor = new SourceTrackingMonitor(logger, "umm")
    val rejected = Failure.rejected("try again")
    monitor.handle(rejected)

    verify(logger).log(mockitoEq(Level.FINE), any(), mockitoEq(rejected))
    verify(logger, never()).log(mockitoEq(Level.WARNING), any(), mockitoEq(rejected))
  }
}
