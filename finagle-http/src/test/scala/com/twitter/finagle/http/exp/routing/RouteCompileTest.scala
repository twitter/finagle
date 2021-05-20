package com.twitter.finagle.http.exp.routing

import com.twitter.finagle.Service
import com.twitter.finagle.exp.routing.{Route => BaseRoute, Request => Req, Response => Rep}
import com.twitter.finagle.http.{Method, Request, Response, Status}
import com.twitter.util.{Await, Future}
import org.scalatest.funsuite.AnyFunSuite

class RouteCompileTest extends AnyFunSuite {

  test("A Route's service has access to path parameters") {
    val underlying: Service[Req[Request], Rep[Response]] =
      Service.mk { req =>
        val params: ParameterMap = req.getOrElse(Fields.ParameterMapField, EmptyParameterMap)
        val rep = params.getParam("id") match {
          case Some(id: IntValue) =>
            val rep = Response()
            rep.setContentString(id.value)
            rep
          case _ =>
            Response(Status.NotFound)
        }
        Future.value(Rep(rep))
      }

    val path = Path(
      Seq(
        Segment.Slash,
        Segment.Constant("users"),
        Segment.Slash,
        Segment.Parameterized(IntParam("id"))))

    val schema = Schema.apply(Method.Get, path)

    val svc: Service[Req[Request], Rep[Response]] =
      BaseRoute(underlying, "get_user", schema)

    val foundRequest = Req(Request("/users/123"))
      .set(Fields.ParameterMapField, MapParameterMap(Map("id" -> IntValue("123", 123))))
    val found = svc(foundRequest)
    assert(Await.result(found).value.status == Status.Ok)

    val notFoundRequest = Req(Request("/users/123"))
      .set(Fields.ParameterMapField, MapParameterMap(Map("id" -> StringValue("123"))))
    val notFound = svc(notFoundRequest)
    assert(Await.result(notFound).value.status == Status.NotFound)
  }

}
