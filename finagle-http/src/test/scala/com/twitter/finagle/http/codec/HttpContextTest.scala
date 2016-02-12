package com.twitter.finagle.http.codec

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.Deadline
import com.twitter.finagle.http.{Message, Method, Request, Version}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HttpContextTest extends FunSuite {

  def newMsg(): Message = Request(Version.Http11, Method.Get, "/")

  test("written request context matches read request context") {
    val m = newMsg()
    val writtenDeadline = Deadline.ofTimeout(5.seconds)
    Contexts.broadcast.let(Deadline, writtenDeadline) {
      HttpContext.write(m)

      // Clear the deadline value in the context
      Contexts.broadcast.letClear(Deadline) {

        // ensure the deadline was cleared
        assert(Deadline.current == None)

        HttpContext.read(m) {
          val readDeadline = Deadline.current.get
          assert(writtenDeadline == readDeadline)
        }
      }
    }
  }

  test("invalid context header value causes context to not be set") {
    val m = newMsg()
    m.headers.set("Finagle-Ctx-com.twitter.finagle.foo", ",,,");
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
}
