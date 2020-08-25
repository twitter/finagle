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

  private val transformer: RequestTransformingFilter[Int, String, Int] =
    new RequestTransformingFilter(i => i)

  // note: in practice each RoutingService shouldn't directly expose the RoutingServiceBuilder, but
  // instead a more user-friendly facade. See how MethodBuilder is used for inspiration.
  def newBuilder: RoutingServiceBuilder[Int, String, SvcSchema, Int] =
    RoutingServiceBuilder
      .newBuilder(RouterBuilder.newBuilder[Int, Route, StringRouter](generator))
      .withNotFoundHandler(_ => Future.const(Return("not found")))
      .withExceptionHandler {
        case ReqRepT(_, Throw(NonFatal(t))) =>
          Future.value(s"${t.getClass.getSimpleName}: ${t.getMessage}")
      }

}

class RoutingServiceTest extends FunSuite {
  import RoutingServiceTest._

  def await[T](awaitable: Awaitable[T]): T = Await.result(awaitable, 1.second)

  test("routes to destinations") {
    val evenRouter: Service[Int, String] = newBuilder
      .withRoute { r =>
        r.withLabel("evens")
          .withSchema(SvcSchema(_ % 2 == 0))
          .withRequestTransformer(transformer)
          .withService(Service.mk[Int, String](i => Future.value(i.toString)))
      }
      .build()
    assert(await(evenRouter(1)) == "not found")
    assert(await(evenRouter(2)) == "2")
    assert(await(evenRouter(-1)) == "IllegalArgumentException: BANG!")

    val oddRouter: Service[Int, String] = newBuilder
      .withRoute { r =>
        r.withLabel("odds")
          .withSchema(SvcSchema(_ % 2 != 0))
          .withRequestTransformer(transformer)
          .withService(Service.mk[Int, String](i => Future.value(i.toString)))
      }
      .withRoute { r =>
        r.withLabel("error")
          .withSchema(SvcSchema(_ == 8))
          .withRequestTransformer(transformer)
          .withService(Service.const(Future.exception(new IllegalStateException("8's BAD!"))))
      }
      .withExceptionHandler {
        case ReqRepT(_, Throw(e: IllegalStateException)) if e.getMessage == "8's BAD!" =>
          Future.value("8's GOOD!")
      }
      .build()

    assert(await(oddRouter(1)) == "1")
    assert(await(oddRouter(2)) == "not found")
    assert(await(oddRouter(8)) == "8's GOOD!")
    intercept[IllegalArgumentException] {
      await(oddRouter(-1)) // we have overridden the default error handler
    }
  }

}
