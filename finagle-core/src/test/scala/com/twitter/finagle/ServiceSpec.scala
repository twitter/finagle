package com.twitter.finagle

import com.twitter.util.{Await, Future, Return, Throw, Time}
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class ServiceSpec extends SpecificationWithJUnit with Mockito {
  "ServiceProxy" should {
    "proxy all requests" in {
      val service = mock[Service[String, String]]
      service.close(any) returns Future.Done
      service.isAvailable returns false

      val proxied = new ServiceProxy(service){}

      there was no(service).close(any)
      there was no(service).isAvailable
      there was no(service)(any)

      proxied.close()
      there was one(service).close(any)
      proxied.isAvailable must beFalse
      there was one(service).isAvailable

      proxied("ok")
      there was one(service)("ok")
    }
  }

  "ServiceFactory.const" should {
    val service = mock[Service[String, String]]
    service.close(any) returns Future.Done
    service("ok") returns Future.value("ko")
    val factory = ServiceFactory.const(service)

    "resolve immediately to the given service" in {
      val f: Future[Service[String, String]] = factory()
      f.isDefined must beTrue
      val proxied = Await.result(f)
      proxied("ok").poll must be_==(Some(Return("ko")))
      there was one(service)("ok")
    }
  }
  
  "ServiceFactory.flatMap" should {
    "release underlying service on failure" in {
      val exc = new Exception
      val service = mock[Service[String, String]]
      service.close(any) returns Future.Done
      val factory = new ServiceFactory[String, String] {
        def apply(conn: ClientConnection) = Future.value(service)
        def close(deadline: Time) = Future.Done
      }

      there was no(service).close(any)
      var didRun = false
      val f2 = factory flatMap { _ =>
        didRun = true
        Future.exception(exc)
      }
      didRun must beFalse
      there was no(service).close(any)

      f2().poll must beSome(Throw(exc))
      didRun must beTrue
      there was one(service).close(any)
    }
  }
}
