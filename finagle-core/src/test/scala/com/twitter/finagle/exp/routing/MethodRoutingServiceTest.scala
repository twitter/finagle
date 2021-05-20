package com.twitter.finagle.exp.routing

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Service
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.util.routing.{
  Found,
  Generator,
  NotFound,
  Result,
  Router,
  RouterBuilder,
  RouterInfo,
  ValidationError,
  Validator
}
import com.twitter.util.{Await, Awaitable, Future, Throw}
import org.scalatest.funsuite.AnyFunSuite

// simulate a Thrift style RoutingService
private object MethodRoutingServiceTest {

  sealed trait Struct

  object MethodField extends MessageField[Method]

  sealed trait Method {
    type Args <: Struct
    type SuccessType <: Struct
  }

  case class UserRequest(id: String) extends Struct
  case class UserResponse(id: String, body: String) extends Struct

  object GetUser extends Method {
    type Args = UserRequest
    type SuccessType = UserResponse
  }

  object DeleteUser extends Method {
    type Args = UserRequest
    type SuccessType = UserResponse
  }

  object UndefinedMethod extends Method {
    type Args = UserRequest
    type SuccessType = UserResponse
  }

  type Route =
    com.twitter.finagle.exp.routing.Route[Struct, Struct, Method]

  private class MethodRouter(
    label: String,
    routes: Iterable[Route])
      extends Router[Request[Struct], Route](label, routes) {
    private[this] val routeMap: Map[Method, Route] = routes.map(r => r.schema -> r).toMap

    protected def find(input: Request[Struct]): Result =
      routeMap.get(
        input.getOrElse(
          MethodField,
          throw new IllegalArgumentException("No MethodField Defined"))) match {
        case Some(route) => Found(input, route)
        case _ => NotFound
      }
  }

  private[this] val validator: Validator[Route] = new Validator[Route] {
    def apply(routes: Iterable[Route]): Iterable[ValidationError] = {
      val methods = routes.map(_.schema).toSet

      val distinctMethods =
        if (methods.size != routes.size) {
          Some(ValidationError("multiple routes defined for same method"))
        } else None

      val allMethodsDefined =
        if (methods == Set(GetUser, DeleteUser)) None
        else Some(ValidationError("missing methods"))

      distinctMethods ++ allMethodsDefined
    }
  }

  private[this] val generator = new Generator[Request[Struct], Route, MethodRouter] {
    def apply(
      routeInfo: RouterInfo[Route]
    ): MethodRouter = new MethodRouter(routeInfo.label, routeInfo.routes)
  }

  private val notFoundHandler: Request[Struct] => Future[Response[Struct]] = r =>
    Future.const(
      Throw(
        new IllegalArgumentException(
          s"Method not defined that can handle request $r for this service")))

  private def newBuilder: RouterBuilder[Request[Struct], Route, MethodRouter] =
    RouterBuilder.newBuilder(generator).withValidator(validator)

}

class MethodRoutingServiceTest extends AnyFunSuite {
  import MethodRoutingServiceTest._

  def await[T](awaitable: Awaitable[T]): T = Await.result(awaitable, 1.second)

  test("routes to destinations") {
    val router = newBuilder
      .withRoute(
        Route(
          label = "get_user",
          schema = GetUser,
          service = Service.mk[Request[GetUser.Args], Response[GetUser.SuccessType]] { req =>
            val id = req.value.id
            Future.value(Response(UserResponse(id, s"Hello, $id")))
          }
        ).asInstanceOf[Route]
      )
      .withRoute(
        Route(
          label = "delete_user",
          schema = DeleteUser,
          service = Service.mk[Request[DeleteUser.Args], Response[DeleteUser.SuccessType]] { req =>
            val id = req.value.id
            Future.value(Response(UserResponse(id, s"Goodbye, $id")))
          }
        ).asInstanceOf[Route]
      )
      .newRouter()

    val svc: Service[Request[Struct], Response[Struct]] = new RoutingService(
      router = router,
      notFoundHandler = notFoundHandler,
      exceptionHandler = PartialFunction.empty,
      statsReceiver = NullStatsReceiver
    )

    assert(
      await(svc(Request(UserRequest("123")).set(MethodField, GetUser))) == Response(
        UserResponse("123", "Hello, 123")))

    assert(
      await(svc(Request(UserRequest("123")).set(MethodField, DeleteUser))) == Response(
        UserResponse("123", "Goodbye, 123")))

    intercept[IllegalArgumentException] {
      await(svc(Request(UserRequest("123")).set(MethodField, UndefinedMethod)))
    }

    intercept[IllegalArgumentException] {
      await(svc(Request(UserRequest("123"))))
    }

  }

}
