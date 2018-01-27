package com.twitter.finagle.client

import com.twitter.conversions.time._
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.service.{ReqRep, ResponseClass, ResponseClassifier}
import com.twitter.logging.{BareFormatter, Level, Logger, StringHandler}
import com.twitter.util.{Await, Future, Return, Stopwatch, Time}
import org.scalatest.{BeforeAndAfter, FunSuite}

class LogFailuresFilterTest extends FunSuite with BeforeAndAfter {

  private[this] val logger = Logger.get()
  private[this] val handler = new StringHandler(BareFormatter, None)

  before {
    handler.clear()
    logger.setLevel(Level.DEBUG)
    logger.clearHandlers()
    logger.addHandler(handler)
  }

  private[this] def filter(
    classifier: ResponseClassifier
  ) = new MethodBuilderRetry.LogFailuresFilter[String, String](
    logger,
    "CoolClient/MopeyMethod",
    classifier,
    Stopwatch.timeMillis
  )

  test("does not log successful responses") {
    val alwaysReturn = Service.const(Future.value("ok"))
    val svc = filter(ResponseClassifier.Default).andThen(alwaysReturn)

    Await.ready(svc("ok"), 5.seconds)
    assert("" == handler.get)
  }

  test("logs failed Throw responses") {
    val alwaysFails = Service.const[String](Future.exception(new RuntimeException()))
    val svc = filter(ResponseClassifier.Default).andThen(alwaysFails)

    Await.ready(svc("nope"), 5.seconds)
    assert(handler.get.startsWith("Request failed for CoolClient/MopeyMethod"))
  }

  test("logs failed Return responses") {
    val alwaysReturn = Service.const(Future.value("ok"))
    val classifier = ResponseClassifier.named("returns-are-failures") {
      case ReqRep(_, Return(_)) => ResponseClass.RetryableFailure
    }
    val svc = filter(classifier).andThen(alwaysReturn)

    Await.result(svc("nope"), 5.seconds)
    assert(handler.get.startsWith("Request failed for CoolClient/MopeyMethod"))
  }

  test("does not log failures at info level") {
    val alwaysFails = Service.const[String](Future.exception(new RuntimeException()))
    val svc = filter(ResponseClassifier.Default).andThen(alwaysFails)
    logger.setLevel(Level.INFO)

    Await.ready(svc("nope"), 5.seconds)
    assert("" == handler.get)
  }

  test("logs include request and response at trace level") {
    val alwaysFails = Service.const[String](Future.exception(new RuntimeException()))
    val svc = filter(ResponseClassifier.Default).andThen(alwaysFails)
    logger.setLevel(Level.TRACE)

    Await.ready(svc("nope"), 5.seconds)
    val log = handler.get
    assert(log.contains("nope"))
    assert(log.contains("RuntimeException"))
  }

  test("logs include the elapsed time") {
    Time.withCurrentTimeFrozen { tc =>
      val alwaysFails = Service.const[String](Future.exception(new RuntimeException()))
      val advanceTime = new SimpleFilter[String, String] {
        def apply(request: String, service: Service[String, String]): Future[String] = {
          tc.advance(5.milliseconds)
          service(request)
        }
      }
      val svc = filter(ResponseClassifier.Default)
        .andThen(advanceTime)
        .andThen(alwaysFails)

      Await.ready(svc("nope"), 5.seconds)
      assert(handler.get.contains("elapsed=5 ms"))
    }
  }

}
