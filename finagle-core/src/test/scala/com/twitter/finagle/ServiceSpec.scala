package com.twitter.finagle

import org.specs.Specification
import org.specs.mock.Mockito

import com.twitter.util.{Future, Return, Throw}

object ServiceSpec extends Specification with Mockito {
  "ServiceProxy" should {
    "proxy all requests" in {
      val service = mock[Service[String, String]]
      service.isAvailable returns false

      val proxied = new ServiceProxy(service){}

      there was no(service).release()
      there was no(service).isAvailable
      there was no(service)(any)

      proxied.release()
      there was one(service).release()
      proxied.isAvailable must beFalse
      there was one(service).isAvailable

      proxied("ok")
      there was one(service)("ok")
    }
  }

  "ServiceFactory.const" should {
    val service = mock[Service[String, String]]
    service("ok") returns Future.value("ko")
    val factory = ServiceFactory.const(service)

    "resolve immediately to the given service" in {
      val f: Future[Service[String, String]] = factory()
      f.isDefined must beTrue
      val proxied = f()
      proxied("ok").poll must be_==(Some(Return("ko")))
      there was one(service)("ok")
    }
  }
  
  "ServiceFactory.flatMap" should {
    "release underlying service on failure" in {
      val exc = new Exception
      val service = mock[Service[String, String]]
      val factory = new ServiceFactory[String, String] {
        def apply(conn: ClientConnection) = Future.value(service)
        def close() = ()
      }

      there was no(service).release()
      var didRun = false
      val f2 = factory flatMap { _ =>
        didRun = true
        Future.exception(exc)
      }
      didRun must beFalse
      there was no(service).release()

      f2().poll must beSome(Throw(exc))
      didRun must beTrue
      there was one(service).release()
    }
  }
}
