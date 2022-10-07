package com.twitter.finagle.client

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Service
import com.twitter.finagle.SimpleFilter
import com.twitter.finagle.service.ReqRep
import com.twitter.finagle.service.ResponseClass
import com.twitter.finagle.service.ResponseClassifier
import com.twitter.logging.Level
import com.twitter.logging.Logger
import com.twitter.util.Await
import com.twitter.util.Future
import com.twitter.util.Return
import com.twitter.util.Stopwatch
import com.twitter.util.Time
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyString
import org.mockito.ArgumentMatchers.anyVararg
import org.mockito.ArgumentMatchers.contains
import org.mockito.Mockito.never
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class LogFailuresFilterTest extends AnyFunSuite with MockitoSugar {

  private[this] def filter(logger: Logger, classifier: ResponseClassifier) =
    new MethodBuilderRetry.LogFailuresFilter[String, String](
      logger,
      "CoolClient/MopeyMethod",
      classifier,
      Stopwatch.timeMillis
    )

  test("does not log successful responses") {
    val logger = mock[Logger]
    when(logger.isLoggable(Level.DEBUG)).thenReturn(true)

    val alwaysReturn = Service.const(Future.value("ok"))
    val svc = filter(logger, ResponseClassifier.Default).andThen(alwaysReturn)

    Await.ready(svc("ok"), 5.seconds)
    verify(logger, never()).debug(any[Throwable](), anyString(), anyVararg())
    verify(logger, never()).debug(anyString(), anyVararg())
  }

  test("logs failed Throw responses") {
    val logger = mock[Logger]
    when(logger.isLoggable(Level.DEBUG)).thenReturn(true)

    val alwaysFails = Service.const[String](Future.exception(new RuntimeException()))
    val svc = filter(logger, ResponseClassifier.Default).andThen(alwaysFails)

    Await.ready(svc("nope"), 5.seconds)

    val expectedMessage = "Request failed for CoolClient/MopeyMethod"
    verify(logger, times(1)).debug(any[Throwable](), contains(expectedMessage), anyVararg())
  }

  test("logs failed Return responses") {
    val logger = mock[Logger]
    when(logger.isLoggable(Level.DEBUG)).thenReturn(true)

    val alwaysReturn = Service.const(Future.value("ok"))
    val classifier = ResponseClassifier.named("returns-are-failures") {
      case ReqRep(_, Return(_)) => ResponseClass.RetryableFailure
    }
    val svc = filter(logger, classifier).andThen(alwaysReturn)

    Await.result(svc("nope"), 5.seconds)

    val expectedMessage = "Request failed for CoolClient/MopeyMethod"
    verify(logger, times(1)).debug(any[Throwable](), contains(expectedMessage), anyVararg())
  }

  test("does not log failures at info level") {
    val logger = mock[Logger]
    // an info level logger would not log debug messages
    when(logger.isLoggable(Level.DEBUG)).thenReturn(false)

    val alwaysFails = Service.const[String](Future.exception(new RuntimeException()))
    val svc = filter(logger, ResponseClassifier.Default).andThen(alwaysFails)

    Await.ready(svc("nope"), 5.seconds)

    verify(logger, never()).debug(any[Throwable](), anyString(), anyVararg())
    verify(logger, never()).info(any[Throwable](), anyString(), anyVararg())
  }

  test("logs include request and response at trace level") {
    val logger = mock[Logger]
    // a trace level logger would log both debug and trace messages
    when(logger.isLoggable(Level.DEBUG)).thenReturn(true)
    when(logger.isLoggable(Level.TRACE)).thenReturn(true)

    val alwaysFails = Service.const[String](Future.exception(new RuntimeException()))
    val svc = filter(logger, ResponseClassifier.Default).andThen(alwaysFails)

    Await.ready(svc("nope"), 5.seconds)

    // Debug Logging is on, but we log at trace instead.
    verify(logger, never()).debug(any[Throwable](), anyString(), anyVararg())

    val expectedMessage = "(request=nope, response=Throw(java.lang.RuntimeException))"
    verify(logger, times(1)).trace(any[Throwable](), contains(expectedMessage), anyVararg())
  }

  test("logs include the elapsed time") {
    val logger = mock[Logger]
    when(logger.isLoggable(Level.DEBUG)).thenReturn(true)

    Time.withCurrentTimeFrozen { tc =>
      val alwaysFails = Service.const[String](Future.exception(new RuntimeException()))
      val advanceTime = new SimpleFilter[String, String] {
        def apply(request: String, service: Service[String, String]): Future[String] = {
          tc.advance(5.milliseconds)
          service(request)
        }
      }
      val svc = filter(logger, ResponseClassifier.Default)
        .andThen(advanceTime)
        .andThen(alwaysFails)

      Await.ready(svc("nope"), 5.seconds)

      verify(logger, times(1)).debug(any[Throwable](), contains("elapsed=5 ms"), anyVararg())
    }
  }

}
