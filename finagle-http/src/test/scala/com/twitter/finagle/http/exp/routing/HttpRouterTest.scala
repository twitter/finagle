package com.twitter.finagle.http.exp.routing

import com.twitter.finagle.Service
import com.twitter.finagle.exp.routing.{Request => Req, Response => Rep, Route => BaseRoute}
import com.twitter.finagle.http.{Method, Request, Response}
import com.twitter.util.Future
import com.twitter.util.routing.{Found, NotFound}
import java.lang.IllegalStateException
import org.scalatest.funsuite.AnyFunSuite

class HttpRouterTest extends AnyFunSuite {

  private[this] val svc: Service[Req[Request], Rep[Response]] =
    Service.const(
      Future.exception(
        new IllegalStateException("Please, don't call the Route's service in these tests.")))

  private[this] val getUsersRoute = BaseRoute(
    service = svc,
    label = "get_users",
    schema = Schema(Method.Get, Path(Seq(Segment.Slash, Segment.Constant("users"))))
  )

  private[this] val getUserByIdRoute = BaseRoute(
    service = svc,
    label = "get_user_by_id",
    schema = Schema(
      Method.Get,
      Path(
        Seq(
          Segment.Slash,
          Segment.Constant("users"),
          Segment.Slash,
          Segment.Parameterized(IntParam("id")))))
  )

  private[this] val deleteUserByIdRoute = BaseRoute(
    service = svc,
    label = "delete_user_by_id",
    schema = Schema(
      Method.Delete,
      Path(
        Seq(
          Segment.Slash,
          Segment.Constant("users"),
          Segment.Slash,
          Segment.Parameterized(IntParam("id")))))
  )

  private[this] val router = HttpRouter
    .newBuilder(LinearPathRouter.Generator)
    .withLabel("test")
    .withRoute(getUsersRoute)
    .withRoute(getUserByIdRoute)
    .withRoute(deleteUserByIdRoute)
    .newRouter()

  test("LinearHttpRouter#finds constant Route") {
    val constantResult = router(Req(Request("/users")))
    constantResult match {
      case Found(input: Req[_], route) =>
        assert(route == getUsersRoute)
        val request = input.asInstanceOf[Req[Request]].value
        assert(request.method == Method.Get)
        assert(request.uri == "/users")
        assert(input.get(Fields.ParameterMapField) == None)
      case result =>
        fail(s"Unexpected Routing Result: $result")
    }
  }

  test("LinearHttpRouter#finds parameterized Route with extracted parameter info") {
    val getResult = router(Req(Request("/users/123")))
    getResult match {
      case Found(input: Req[_], route) =>
        assert(route == getUserByIdRoute)
        val request: Request = input.asInstanceOf[Req[Request]].value
        assert(request.method == Method.Get)
        assert(request.uri == "/users/123")
        assert(
          input.get(Fields.ParameterMapField) == Some(
            MapParameterMap(Map("id" -> IntValue("123", 123)))))
      case result =>
        fail(s"Unexpected Routing Result: $result")
    }
  }

  test(
    "LinearHttpRouter#returns NotFound for a request where no route contains the specified path") {
    assert(router(Req(Request("/abc"))) == NotFound)
    assert(router(Req(Request("/users/"))) == NotFound) // trailing slash is significant
    assert(router(Req(Request("/users/forty"))) == NotFound) // 'forty' doesn't match our IntParam
  }

  test("LinearHttpRouter#Query Params aren't taken into consideration when routing") {
    val constantResult = router(Req(Request("/users?param1=value1&param2=value2")))
    constantResult match {
      case Found(input: Req[_], route) =>
        assert(route == getUsersRoute)
        val request = input.asInstanceOf[Req[Request]].value
        assert(request.method == Method.Get)
        assert(request.uri == "/users?param1=value1&param2=value2")
        assert(input.get(Fields.ParameterMapField) == None)
        assert(input.get(Fields.PathField) == Some("/users"))
      case result =>
        fail(s"Unexpected Routing Result: $result")
    }

    val getResult = router(Req(Request("/users/123?param1=value1&param2=value2")))
    getResult match {
      case Found(input: Req[_], route) =>
        assert(route == getUserByIdRoute)
        val request: Request = input.asInstanceOf[Req[Request]].value
        assert(request.method == Method.Get)
        assert(request.uri == "/users/123?param1=value1&param2=value2")
        assert(
          input.get(Fields.ParameterMapField) == Some(
            MapParameterMap(Map("id" -> IntValue("123", 123)))))
        assert(input.get(Fields.PathField) == Some("/users/123"))
      case result =>
        fail(s"Unexpected Routing Result: $result")
    }
  }

  test(
    "LinearHttpRouter#finds correct Route when multiple Routes exist for same Path with different Method") {
    val getResult = router(Req(Request("/users/123")))
    getResult match {
      case Found(input: Req[_], route) =>
        assert(route == getUserByIdRoute)
        val request: Request = input.asInstanceOf[Req[Request]].value
        assert(request.method == Method.Get)
        assert(request.uri == "/users/123")
        assert(
          input.get(Fields.ParameterMapField) == Some(
            MapParameterMap(Map("id" -> IntValue("123", 123)))))
      case result =>
        fail(s"Unexpected Routing Result: $result")
    }

    val deleteResult = router(Req(Request(Method.Delete, "/users/123")))
    deleteResult match {
      case Found(input: Req[_], route) =>
        assert(route == deleteUserByIdRoute)
        val request: Request = input.asInstanceOf[Req[Request]].value
        assert(request.method == Method.Delete)
        assert(request.uri == "/users/123")
        assert(
          input.get(Fields.ParameterMapField) == Some(
            MapParameterMap(Map("id" -> IntValue("123", 123)))))
      case result =>
        fail(s"Unexpected Routing Result: $result")
    }
  }

  test(
    "LinearHttpRouter#throws MethodNotAllowed exception when one or more Routes exist " +
      "for a Path, but does not match a Method that is associated with that Path") {
    intercept[MethodNotAllowedException] {
      router(Req(Request(Method.Post, "/users/123")))
    }

    intercept[MethodNotAllowedException] {
      router(Req(Request(Method.Delete, "/users")))
    }
  }

  test(
    "HttpRouter#cannot build a Router where multiple routes exist for the same Method and Path") {
    intercept[IllegalArgumentException] {
      HttpRouter
        .newBuilder(LinearPathRouter.Generator)
        .withLabel("test")
        .withRoute(getUsersRoute)
        .withRoute(getUserByIdRoute)
        .withRoute(deleteUserByIdRoute)
        .withRoute(getUsersRoute.copy(label = "duplicate"))
        .newRouter()
    }
  }

}
