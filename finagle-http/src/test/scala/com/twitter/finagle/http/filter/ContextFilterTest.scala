package com.twitter.finagle.http.filter

import com.twitter.conversions.time._
import com.twitter.finagle.{Deadline, Service}
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.http.{Status, Response, Request}
import com.twitter.finagle.http.codec.HttpContext
import com.twitter.util.{Await, Future}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ContextFilterTest extends FunSuite {

  test("parses Finagle-Ctx headers") {
    val writtenDeadline = Deadline.ofTimeout(5.seconds)
    val service =
      new ClientContextFilter[Request, Response] andThen
      new ServerContextFilter[Request, Response] andThen
      Service.mk[Request, Response] { req =>
        assert(Deadline.current.get == writtenDeadline)
        Future.value(Response())
      }

    Contexts.broadcast.let(Deadline, writtenDeadline) {
      val req = Request()
      HttpContext.write(req)

      // Clear the deadline value in the context
      Contexts.broadcast.letClear(Deadline) {
        // ensure the deadline was cleared
        assert(Deadline.current == None)

        val rsp = Await.result(service(req))
        assert(rsp.status == Status.Ok)
      }
    }
  }

  test("does not set incorrectly encoded context headers") {
    val service =
      new ClientContextFilter[Request, Response] andThen
      new ServerContextFilter[Request, Response] andThen
      Service.mk[Request, Response] { _ =>
        assert(Contexts.broadcast.marshal.isEmpty)
        Future.value(Response())
      }

    val req = Request()
    req.headers().add("Finagle-Ctx-com.twitter.finagle.Deadline", "foo")

    val rsp = Await.result(service(req))
    assert(rsp.status == Status.Ok)
  }
}
