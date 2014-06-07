package com.twitter.finagle.builder

import com.twitter.finagle.{Failure, RequestException}
import java.util.logging.{Level, Logger}
import org.junit.runner.RunWith
import org.mockito.Mockito.verify
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class SourceTrackingMonitorTest extends FunSuite with MockitoSugar {
  test("handles unrolling properly") {
    val logger = mock[Logger]
    val monitor = new SourceTrackingMonitor(logger, "qux")
    val e = new Exception
    val f1 = new Failure("foo", e, sources = Map(Failure.Sources.ServiceName -> "tweet"))
    val f2 = new Failure("bar", f1)
    val exc = new RequestException(f2)
    exc.serviceName = "user"
    monitor.handle(exc)
    verify(logger).log(
      Level.SEVERE,
      "A qux service " +
        Seq("user", "tweet").mkString(" on behalf of ") +
        " threw an exception",
      exc
    )
  }
}
