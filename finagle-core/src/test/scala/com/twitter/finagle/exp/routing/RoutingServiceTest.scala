package com.twitter.finagle.exp.routing

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Service
import com.twitter.finagle.service.ReqRepT
import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver}
import com.twitter.util.routing.{
  Found,
  Generator,
  NotFound,
  Result,
  Router,
  RouterBuilder,
  RouterInfo
}
import com.twitter.util.{Await, Awaitable, Future, Return, Throw}
import org.scalatest.FunSuite
import scala.util.control.NonFatal

private object RoutingServiceTest {

  case class SvcSchema(fn: Int => Boolean)

  type Route = com.twitter.finagle.exp.routing.Route[Int, String, SvcSchema]

  class StringRouter(
    label: String,
    routes: Iterable[Route])
      extends Router[Int, Route](label, routes) {
    protected def find(input: Int): Result =
      if (input < 0) throw new IllegalArgumentException("BANG!")
      else {
        routes.find(_.schema.fn(input)) match {
          case Some(route) => Found(input, route)
          case _ => NotFound
        }
      }
  }

  private[this] val generator = new Generator[Int, Route, StringRouter] {
    def apply(routerInfo: RouterInfo[Route]): StringRouter =
      new StringRouter(routerInfo.label, routerInfo.routes)
  }

  private val notFoundHandler: Int => Future[String] = _ => Future.const(Return("not found"))

  private val defaultExceptionHandler: PartialFunction[
    ReqRepT[Int, String],
    Future[String]
  ] = {
    case ReqRepT(_, Throw(NonFatal(t))) =>
      Future.value(s"${t.getClass.getSimpleName}: ${t.getMessage}")
  }

  private def newBuilder: RouterBuilder[Int, Route, StringRouter] =
    RouterBuilder.newBuilder(generator)

}

class RoutingServiceTest extends FunSuite {
  import RoutingServiceTest._

  def await[T](awaitable: Awaitable[T]): T = Await.result(awaitable, 1.second)

  test("routes to destinations") {
    val evenRouter = newBuilder
      .withRoute(
        Route(
          label = "evens",
          schema = SvcSchema(_ % 2 == 0),
          service = Service.mk[Int, String](i => Future.value(i.toString))
        )
      )
      .newRouter()

    val evenSvc: Service[Int, String] = new RoutingService(
      router = evenRouter,
      notFoundHandler = notFoundHandler,
      exceptionHandler = defaultExceptionHandler,
      statsReceiver = NullStatsReceiver)

    assert(await(evenSvc(1)) == "not found")
    assert(await(evenSvc(2)) == "2")
    assert(await(evenSvc(-1)) == "IllegalArgumentException: BANG!")

    val customExceptionHandler: PartialFunction[ReqRepT[Int, String], Future[String]] = {
      case ReqRepT(_, Throw(e: IllegalStateException)) if e.getMessage == "8's BAD!" =>
        Future.value("8's GOOD!")
    }

    val oddRouter = newBuilder
      .withRoute(
        Route(
          label = "odds",
          schema = SvcSchema(_ % 2 != 0),
          service = Service.mk[Int, String](i => Future.value(i.toString))
        )
      )
      .withRoute(
        Route(
          label = "error",
          schema = SvcSchema(_ == 8),
          service = Service.const(Future.exception(new IllegalStateException("8's BAD!")))
        )
      )
      .newRouter()

    val oddSvc: Service[Int, String] = new RoutingService(
      router = oddRouter,
      notFoundHandler = notFoundHandler,
      exceptionHandler = customExceptionHandler,
      statsReceiver = NullStatsReceiver
    ) // override the exception handler for our test

    assert(await(oddSvc(1)) == "1")
    assert(await(oddSvc(2)) == "not found")
    assert(await(oddSvc(8)) == "8's GOOD!")
    intercept[IllegalArgumentException] {
      await(oddSvc(-1)) // we have overridden the default error handler
    }
  }

  test("exposes counters for underlying behavior") {
    val router = newBuilder
      .withLabel("even")
      .withRoute(
        Route(
          label = "evens",
          schema = SvcSchema(_ % 2 == 0),
          service = Service.mk[Int, String](i => Future.value(i.toString))
        )
      )
      .withRoute(
        Route(
          label = "handled_error",
          schema = SvcSchema(_ == 3),
          service = Service.const(Future.exception(new IllegalStateException("3's BAD!")))
        )
      )
      .withRoute(
        Route(
          label = "unhandled_error",
          schema = SvcSchema(_ == 5),
          service = Service.const(Future.exception(new IllegalStateException("number 5 :(")))
        )
      )
      .newRouter()

    val stats = new InMemoryStatsReceiver

    val customExceptionHandler: PartialFunction[ReqRepT[Int, String], Future[String]] = {
      case ReqRepT(_, Throw(e: IllegalStateException)) if e.getMessage == "3's BAD!" =>
        Future.value("3's GOOD!")
    }

    val svc: Service[Int, String] = new RoutingService(
      router = router,
      notFoundHandler = notFoundHandler,
      exceptionHandler = customExceptionHandler,
      statsReceiver = stats)

    val found = stats.counter("router", "found")
    val notFound = stats.counter("router", "not_found")
    val handledFailures = stats.counter("router", "failures", "handled")
    val unhandledFailures = stats.counter("router", "failures", "unhandled")

    // 1 is odd, so won't be found and triggers the notFoundHandler and returns successfully
    await(svc(1))
    assert(notFound() == 1)
    assert(found() == 0)
    assert(handledFailures() == 0)
    assert(unhandledFailures() == 0)

    // 2 is even, it will be found and return a successful response
    await(svc(2))
    assert(notFound() == 1)
    assert(found() == 1)
    assert(handledFailures() == 0)
    assert(unhandledFailures() == 0)

    // 5 should trigger the exceptionHandler and result in an unhandled error
    intercept[IllegalStateException] { await(svc(5)) }
    assert(notFound() == 1)
    assert(found() == 2)
    assert(handledFailures() == 0)
    assert(unhandledFailures() == 1)

    // 3 should trigger the exceptionHandler, be handled, and return a successful response
    await(svc(3))
    assert(notFound() == 1)
    assert(found() == 3)
    assert(handledFailures() == 1)
    assert(unhandledFailures() == 1)
  }

}
