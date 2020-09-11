package com.twitter.finagle.exp.routing

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Service
import com.twitter.finagle.service.ReqRepT
import com.twitter.util.routing.{Generator, Router, RouterBuilder}
import com.twitter.util.{Await, Awaitable, Future, Return, Throw}
import org.scalatest.FunSuite
import scala.util.control.NonFatal

private object RoutingServiceTest {

  case class SvcSchema(fn: Int => Boolean)

  type Route = com.twitter.finagle.exp.routing.Route[Int, String, SvcSchema]

  case class StringRouter(
    label: String,
    routes: Iterable[Route])
      extends Router[Int, Route] {
    override protected def find(input: Int): Option[Route] =
      if (input < 0) throw new IllegalArgumentException("BANG!")
      else {
        routes.find(_.schema.fn(input))
      }
  }

  private[this] val generator = new Generator[Int, Route, StringRouter] {
    override def apply(
      label: String,
      routes: Iterable[Route]
    ): StringRouter = StringRouter(label, routes)
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
      exceptionHandler = defaultExceptionHandler)

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
      exceptionHandler = customExceptionHandler
    ) // override the exception handler for our test

    assert(await(oddSvc(1)) == "1")
    assert(await(oddSvc(2)) == "not found")
    assert(await(oddSvc(8)) == "8's GOOD!")
    intercept[IllegalArgumentException] {
      await(oddSvc(-1)) // we have overridden the default error handler
    }
  }

}
