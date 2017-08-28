package com.twitter.finagle.http.codec

import com.twitter.conversions.time._
import com.twitter.finagle.context.{Contexts, Deadline, Retries}
import com.twitter.finagle.http.{Message, Method, Request, Version}
import org.scalatest.FunSuite

class HttpContextTest extends FunSuite {

  def newMsg(): Message = Request(Version.Http11, Method.Get, "/")

  test("written request deadline matches read request deadline") {
    val m = newMsg()
    val writtenDeadline = Deadline.ofTimeout(5.seconds)
    Contexts.broadcast.let(Deadline, writtenDeadline) {
      HttpContext.write(m)

      // Clear Deadline in the context
      Contexts.broadcast.letClear(Deadline) {

        // Ensure the Deadline was cleared
        assert(Deadline.current == None)

        HttpContext.read(m) {
          val readDeadline = Deadline.current.get
          assert(writtenDeadline == readDeadline)
        }
      }
    }
  }

  test("written request retries matches read request retries") {
    val m = newMsg()
    val writtenRetries = Retries(5)
    Contexts.broadcast.let(Retries, writtenRetries) {
      HttpContext.write(m)

      // Clear Retries in the Context
      Contexts.broadcast.letClear(Retries) {

        // Ensure the Retries was cleared
        assert(Retries.current == None)

        HttpContext.read(m) {
          val readRetries = Retries.current.get
          assert(writtenRetries == readRetries)
        }
      }
    }
  }

  test("invalid context header value causes context to not be set") {
    val m = newMsg()
    m.headerMap.set("Finagle-Ctx-com.twitter.finagle.foo", ",,,")
    HttpContext.read(m) {
      assert(Contexts.broadcast.marshal.isEmpty)
    }
  }

  test("when there are no context headers, reading returns an empty iterator") {
    val m = newMsg()
    HttpContext.read(m) {
      assert(Contexts.broadcast.marshal.isEmpty)
    }
  }

  test("removing deadline") {
    val m = newMsg()
    val deadlineKey = "Finagle-Ctx-com.twitter.finagle.Deadline"
    Contexts.broadcast.let(Deadline, Deadline.ofTimeout(5.seconds)) {
      HttpContext.write(m)
      assert(m.headerMap.contains(deadlineKey))
      HttpContext.removeDeadline(m)
      assert(!m.headerMap.contains(deadlineKey))
    }
  }
}
