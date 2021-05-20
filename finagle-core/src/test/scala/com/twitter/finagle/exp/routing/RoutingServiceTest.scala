package com.twitter.finagle.exp.routing

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.{Service, SimpleFilter}
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
import scala.util.control.NonFatal
import org.scalatest.funsuite.AnyFunSuite

private object RoutingServiceTest {

  case class SvcSchema(fn: Int => Boolean)

  type Route = com.twitter.finagle.exp.routing.Route[Int, String, SvcSchema]

  object CustomField extends RouteField[String]

  class StringRouter(
    label: String,
    routes: Iterable[Route])
      extends Router[Request[Int], Route](label, routes) {
    protected def find(input: Request[Int]): Result =
      if (input.value < 0) throw new IllegalArgumentException("BANG!")
      else {
        routes.find(_.schema.fn(input.value)) match {
          case Some(route) => Found(input, route)
          case _ => NotFound
        }
      }
  }

  private[this] val generator = new Generator[Request[Int], Route, StringRouter] {
    def apply(routerInfo: RouterInfo[Route]): StringRouter =
      new StringRouter(routerInfo.label, routerInfo.routes)
  }

  private val notFoundHandler: Request[Int] => Future[Response[String]] = _ =>
    Future.const(Return(Response("not found")))

  private val defaultExceptionHandler: PartialFunction[
    ReqRepT[Request[Int], Response[String]],
    Future[Response[String]]
  ] = {
    case ReqRepT(_, Throw(NonFatal(t))) =>
      Future.value(Response(s"${t.getClass.getSimpleName}: ${t.getMessage}"))
  }

  private def newBuilder: RouterBuilder[Request[Int], Route, StringRouter] =
    RouterBuilder.newBuilder(generator)

}

class RoutingServiceTest extends AnyFunSuite {
  import RoutingServiceTest._

  def await[T](awaitable: Awaitable[T]): T = Await.result(awaitable, 1.second)

  test("routes to destinations") {
    val evenRouter = newBuilder
      .withRoute(
        Route.wrap(
          label = "evens",
          schema = SvcSchema(_ % 2 == 0),
          service = Service.mk[Int, String](i => Future.value(i.toString))
        )
      )
      .newRouter()

    val evenSvc: Service[Int, String] = new ReqRepToRequestResponseFilter().andThen(
      new RoutingService(
        router = evenRouter,
        notFoundHandler = notFoundHandler,
        exceptionHandler = defaultExceptionHandler,
        statsReceiver = NullStatsReceiver))

    assert(await(evenSvc(1)) == "not found")
    assert(await(evenSvc(2)) == "2")
    assert(await(evenSvc(-1)) == "IllegalArgumentException: BANG!")

    val customExceptionHandler: PartialFunction[ReqRepT[Request[Int], Response[String]], Future[
      Response[String]
    ]] = {
      case ReqRepT(_, Throw(e: IllegalStateException)) if e.getMessage == "8's BAD!" =>
        Future.value(Response("8's GOOD!"))
    }

    val oddRouter = newBuilder
      .withRoute(
        Route.wrap(
          label = "odds",
          schema = SvcSchema(_ % 2 != 0),
          service = Service.mk[Int, String](i => Future.value(i.toString))
        )
      )
      .withRoute(
        Route.wrap(
          label = "error",
          schema = SvcSchema(_ == 8),
          service = Service.const(Future.exception(new IllegalStateException("8's BAD!")))
        )
      )
      .newRouter()

    val oddSvc: Service[Request[Int], Response[String]] = new RoutingService(
      router = oddRouter,
      notFoundHandler = notFoundHandler,
      exceptionHandler = customExceptionHandler,
      statsReceiver = NullStatsReceiver
    ) // override the exception handler for our test

    assert(await(oddSvc(Request(1))) == Response("1"))
    assert(await(oddSvc(Request(2))) == Response("not found"))
    assert(await(oddSvc(Request(8))) == Response("8's GOOD!"))
    intercept[IllegalArgumentException] {
      await(oddSvc(Request(-1))) // we have overridden the default error handler
    }
  }

  test("can access RouteInfo field via Request wrapper after route matching") {
    // we create a filter that we will apply to the route, which will only be exercised
    // *after* route matching has occurred, within the context of the RoutingService
    val routeInfoFilter = new SimpleFilter[Request[Int], Response[String]] {
      override def apply(
        request: Request[Int],
        service: Service[Request[Int], Response[String]]
      ): Future[Response[String]] = request.get(Fields.RouteInfo) match {
        case Some(route) =>
          service(request).map { rep =>
            Response(s"${route.label}: ${rep.value}")
          }
        case _ =>
          Future.exception(new IllegalStateException("ERROR! No RouteInfo Set!"))
      }
    }

    val underlying = Service.mk[Request[Int], Response[String]] { req: Request[Int] =>
      Future.value(Response(req.value.toString))
    }

    val evenRoute = Route(
      label = "evens",
      schema = SvcSchema(_ % 2 == 0),
      service = routeInfoFilter.andThen(underlying)
    )

    val evenRouter = newBuilder
      .withRoute(evenRoute)
      .newRouter()

    val evenSvc: Service[Int, String] = new ReqRepToRequestResponseFilter().andThen(
      new RoutingService(
        router = evenRouter,
        notFoundHandler = notFoundHandler,
        exceptionHandler = defaultExceptionHandler,
        statsReceiver = NullStatsReceiver))

    assert(await(evenSvc(1)) == "not found")
    assert(await(evenSvc(2)) == "evens: 2") // the filter and route info will only be present here
    assert(await(evenSvc(-1)) == "IllegalArgumentException: BANG!")

    // if we call the route's service outside of the RoutingService/without RouteInfo,
    // the filter will error
    intercept[IllegalStateException] {
      await(evenRoute(Request(1)))
    }
  }

  test("exposes counters for underlying Router behavior") {
    val router = newBuilder
      .withLabel("even")
      .withRoute(
        Route.wrap(
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

    val customExceptionHandler: PartialFunction[ReqRepT[Request[Int], Response[String]], Future[
      Response[String]
    ]] = {
      case ReqRepT(_, Throw(e: IllegalStateException)) if e.getMessage == "3's BAD!" =>
        Future.value(Response("3's GOOD!"))
    }

    val svc: Service[Request[Int], Response[String]] = new RoutingService(
      router = router,
      notFoundHandler = notFoundHandler,
      exceptionHandler = customExceptionHandler,
      statsReceiver = stats)

    val found = stats.counter("router", "found")
    val notFound = stats.counter("router", "not_found")
    val handledFailures = stats.counter("router", "failures", "handled")
    val unhandledFailures = stats.counter("router", "failures", "unhandled")

    // 1 is odd, so won't be found and triggers the notFoundHandler and returns successfully
    await(svc(Request(1)))
    assert(notFound() == 1)
    assert(found() == 0)
    assert(handledFailures() == 0)
    assert(unhandledFailures() == 0)

    // 2 is even, it will be found and return a successful response
    await(svc(Request(2)))
    assert(notFound() == 1)
    assert(found() == 1)
    assert(handledFailures() == 0)
    assert(unhandledFailures() == 0)

    // 5 should trigger the exceptionHandler and result in an unhandled error
    intercept[IllegalStateException] { await(svc(Request(5))) }
    assert(notFound() == 1)
    assert(found() == 2)
    assert(handledFailures() == 0)
    assert(unhandledFailures() == 1)

    // 3 should trigger the exceptionHandler, be handled, and return a successful response
    await(svc(Request(3)))
    assert(notFound() == 1)
    assert(found() == 3)
    assert(handledFailures() == 1)
    assert(unhandledFailures() == 1)
  }

  test("routes can define and access custom fields") {
    val evenRouter = newBuilder
      .withRoute(
        Route
          .wrap(
            label = "evens",
            schema = SvcSchema(_ % 2 == 0),
            service = Service.mk[Int, String](i => Future.value(i.toString))
          ).set(CustomField, "evens, not odds")
      )
      .newRouter()

    evenRouter(Request(4)) match {
      case Found(_, route) =>
        val field = route
          .asInstanceOf[Route].getOrElse(
            CustomField,
            throw new IllegalStateException("No CustomField present on route!"))
        assert(field == "evens, not odds")
      case error => fail(s"received unexpected result: $error")
    }
  }

}
