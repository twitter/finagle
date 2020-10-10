package com.twitter.finagle.exp.routing

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Service
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.util.routing.{Generator, Router, RouterBuilder, ValidationError, Validator}
import com.twitter.util.{Await, Awaitable, Future, Throw}
import org.scalatest.FunSuite

// simulate a Thrift style RoutingService
private object MethodRoutingServiceTest {

  sealed trait Request
  sealed trait Response

  // simulate Thrift's MethodMetadata
  val Key = Contexts.local.newKey[Method]
  def activeMethod: Option[Method] = Contexts.local.get(Key)

  sealed trait Method {
    type Args <: Request
    type SuccessType
    def asCurrent[T](f: => T): T = Contexts.local.let(Key, this)(f)
  }

  case class UserRequest(id: String) extends Request
  case class UserResponse(id: String, body: String) extends Response

  object GetUser extends Method {
    type Args = UserRequest
    type SuccessType = UserResponse
  }

  object DeleteUser extends Method {
    type Args = UserRequest
    type SuccessType = UserResponse
  }

  case class MethodRequest[T <: Request](method: Method, request: T)

  case class MethodSchema(method: Method)

  type Route =
    com.twitter.finagle.exp.routing.Route[Request, Response, MethodSchema]

  private case class MethodRouter(
    label: String,
    routes: Iterable[Route])
      extends Router[Request, Route] {
    private[this] val routeMap: Map[Method, Route] = routes.map(r => r.schema.method -> r).toMap

    override protected def find(input: Request): Option[Route] =
      activeMethod.flatMap { method =>
        routeMap.get(method)
      }
  }

  private[this] val validator: Validator[Route] = new Validator[Route] {
    override def apply(routes: Iterable[Route]): Iterable[ValidationError] = {
      val methods = routes.map(_.schema.method).toSet

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

  private[this] val generator = new Generator[Request, Route, MethodRouter] {
    override def apply(
      label: String,
      routes: Iterable[Route]
    ): MethodRouter = MethodRouter(label, routes)
  }

  private val notFoundHandler: Request => Future[Response] = r =>
    Future.const(
      Throw(
        new IllegalArgumentException(
          s"Method not defined that can handle request $r for this service")))

  private def newBuilder: RouterBuilder[Request, Route, MethodRouter] =
    RouterBuilder.newBuilder(generator).withValidator(validator)

}

class MethodRoutingServiceTest extends FunSuite {
  import MethodRoutingServiceTest._

  def await[T](awaitable: Awaitable[T]): T = Await.result(awaitable, 1.second)

  test("routes to destinations") {
    val router = newBuilder
      .withRoute(
        Route.transformed[Request, Response, MethodSchema, MethodRequest[_]](
          transformer = new RequestTransformingFilter(req => MethodRequest(GetUser, req)),
          route = Route(
            label = "get_user",
            schema = MethodSchema(GetUser),
            service = Service.mk { req =>
              val id = req.request.asInstanceOf[GetUser.Args].id
              Future.value(UserResponse(id, s"Hello, $id"))
            }
          )
        )
      )
      .withRoute(
        Route.transformed[Request, Response, MethodSchema, MethodRequest[_]](
          transformer = new RequestTransformingFilter(req => MethodRequest(DeleteUser, req)),
          route = Route(
            label = "delete_user",
            schema = MethodSchema(DeleteUser),
            service = Service.mk { req =>
              val id = req.request.asInstanceOf[DeleteUser.Args].id
              Future.value(UserResponse(id, s"Goodbye, $id"))
            }
          )
        )
      )
      .newRouter()

    val svc: Service[Request, Response] = new RoutingService(
      router = router,
      notFoundHandler = notFoundHandler,
      exceptionHandler = PartialFunction.empty,
      statsReceiver = NullStatsReceiver
    )

    GetUser.asCurrent {
      assert(await(svc(UserRequest("123"))) == UserResponse("123", "Hello, 123"))
    }

    DeleteUser.asCurrent {
      assert(await(svc(UserRequest("123"))) == UserResponse("123", "Goodbye, 123"))
    }

    intercept[IllegalArgumentException] {
      await(svc(UserRequest("123")))
    }
  }

}
