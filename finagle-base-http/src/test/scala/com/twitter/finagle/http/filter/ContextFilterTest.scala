package com.twitter.finagle.http.filter

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Service
import com.twitter.finagle.context.{Contexts, Deadline, Retries}
import com.twitter.finagle.http.codec.context.HttpContext
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.util.{Await, Future}
import org.scalatest.funsuite.AnyFunSuite

class ContextFilterTest extends AnyFunSuite {

  test("parses Finagle-Ctx headers") {
    val writtenDeadline = Deadline.ofTimeout(5.seconds)
    val writtenRetries = Retries(5)
    val service =
      new ClientContextFilter[Request, Response] andThen
        new ServerContextFilter[Request, Response] andThen
        Service.mk[Request, Response] { req =>
          assert(Deadline.current.get == writtenDeadline)
          assert(Retries.current.get == writtenRetries)
          Future.value(Response())
        }

    Contexts.broadcast.let(Deadline, writtenDeadline) {
      Contexts.broadcast.let(Retries, writtenRetries) {
        val req = Request()
        HttpContext.write(req)

        // Clear the deadline/retries values in the context
        Contexts.broadcast.letClearAll {
          // ensure the deadline was cleared
          assert(Deadline.current == None)

          // ensure the retries was cleared
          assert(Retries.current == None)

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
