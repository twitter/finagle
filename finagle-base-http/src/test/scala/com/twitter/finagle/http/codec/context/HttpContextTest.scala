package com.twitter.finagle.http.codec.context

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.context.{BackupRequest, Contexts, Deadline, Retries}
import com.twitter.finagle.http.{Message, Method, Request, Version}
import com.twitter.io.Buf
import com.twitter.util.Try
import org.scalatest.funsuite.AnyFunSuite

// This context type is loaded in via the `LoadService` mechanism
case class Name(name: String)

object Name extends Contexts.broadcast.Key[Name]("com.twitter.finagle.http.codec.Name") {
  def current: Option[Name] = Contexts.broadcast.get(Name)

  def marshal(n: Name): Buf = {
    Buf.ByteArray.Owned(n.name.getBytes())
  }

  def tryUnmarshal(buf: Buf): Try[Name] = {
    Try {
      Name(
        new String(Buf.ByteArray.Owned.extract(buf))
      )
    }
  }
}

// This class definition must be included into jar's resources via the file,
// `c.t.f.http.codec.context.LoadableHttpContext`, under META-INF/services
// directory so that LoadService can pickup this definition at runtime. See
// the resources of this target as an example.
class LoadedName extends LoadableHttpContext {
  type ContextKeyType = Name
  val key: Contexts.broadcast.Key[ContextKeyType] = Name
}

class HttpContextTest extends AnyFunSuite {

  def newMsg(): Message = Request(Version.Http11, Method.Get, "/")

  test("writes custom broadcast context types via LoadService") {
    val m = newMsg()
    val name = Name("kobe")
    Contexts.broadcast.let(Name, name) {
      HttpContext.write(m)

      Contexts.broadcast.letClear(Name) {
        assert(Name.current == None)

        HttpContext.read(m) {
          val n = Name.current.get
          assert(n.name == "kobe")
        }
      }

    }
  }

  test("custom context types are also prefixed") {
    val m = newMsg()
    val name = Name("bryant")
    Contexts.broadcast.let(Name, name) {
      HttpContext.write(m)

      val hm = m.headerMap
      assert(!hm.isEmpty)
      assert(hm.keySet.forall(_.startsWith(HttpContext.Prefix)))
    }
  }

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

  test("headers are set/replaced, not added") {
    val m = newMsg()
    Contexts.broadcast.let(Retries, Retries(5)) {
      HttpContext.write(m)
    }

    assert(m.headerMap.getAll(HttpRetries.headerKey).size == 1)
    HttpContext.read(m) {
      assert(Contexts.broadcast.get(HttpRetries.key) == Some(Retries(5)))
    }

    // and again...
    Contexts.broadcast.let(Retries, Retries(4)) {
      HttpContext.write(m)
    }

    // Still just 1...
    assert(m.headerMap.getAll(HttpRetries.headerKey).size == 1)

    // Should be the last entry written
    HttpContext.read(m) {
      assert(Contexts.broadcast.get(HttpRetries.key) == Some(Retries(4)))
    }
  }

  test("BackupRequest written matches read") {
    val m = newMsg()
    assert(!BackupRequest.wasInitiated)
    BackupRequest.let {
      HttpContext.write(m)
      assert(BackupRequest.wasInitiated)

      Contexts.broadcast.letClearAll {
        assert(!BackupRequest.wasInitiated)

        HttpContext.read(m) {
          assert(BackupRequest.wasInitiated)
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
