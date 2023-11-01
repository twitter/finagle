package com.twitter.finagle.http.filter

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Service
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.context.Deadline
import com.twitter.finagle.context.Requeues
import com.twitter.finagle.http.codec.context.HttpContext
import com.twitter.finagle.http.Request
import com.twitter.finagle.http.Response
import com.twitter.finagle.http.Status
import com.twitter.util.Await
import com.twitter.util.Future
import org.scalatest.funsuite.AnyFunSuite

class ContextFilterTest extends AnyFunSuite {

  test("parses Finagle-Ctx headers") {
    val writtenDeadline = Deadline.ofTimeout(5.seconds)
    val writtenRequeues = Requeues(5)
    val service =
      new ClientContextFilter[Request, Response] andThen
        new ServerContextFilter[Request, Response] andThen
        Service.mk[Request, Response] { req =>
          assert(Deadline.current.get == writtenDeadline)
          assert(Requeues.current.get == writtenRequeues)
          Future.value(Response())
        }

    Contexts.broadcast.let(Deadline, writtenDeadline) {
      Contexts.broadcast.let(Requeues, writtenRequeues) {
        val req = Request()
        HttpContext.write(req)

        // Clear the deadline/requeues values in the context
        Contexts.broadcast.letClearAll {
          // ensure the deadline was cleared
          assert(Deadline.current == None)

          // ensure the requeues was cleared
          assert(Requeues.current == None)

          val rsp = Await.result(service(req))
          assert(rsp.status == Status.Ok)
        }
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
    req.headerMap.add("Finagle-Ctx-com.twitter.finagle.Deadline", "foo")

    val rsp = Await.result(service(req))
    assert(rsp.status == Status.Ok)
  }
}
