package com.twitter.finagle.service

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

import com.twitter.util.{Future, Promise, Return, Throw}
import com.twitter.finagle.{CancelledConnectionException, Service}

class ProxyServiceSpec extends SpecificationWithJUnit with Mockito {
  "ProxyService" should {
    val underlying = mock[Service[Int, Int]]

    "proxy all methods to the underlying service" in {
      val proxy = new ProxyService(Future.value(underlying))

      val future = mock[Future[Int]]
      underlying(123) returns future

      proxy(123) must be_==(future)
      there was one(underlying)(123)

      underlying.isAvailable returns false
      proxy.isAvailable must be_==(false)
      there was one(underlying).isAvailable

      proxy.release()
      there was one(underlying).release()
    }

    "buffer requests" in {
      val promise = new Promise[Service[Int, Int]]
      val proxy = new ProxyService(promise)

      val f123 = proxy(123)
      val f321 = proxy(321)

      f123.isDefined must beFalse
      f321.isDefined must beFalse

      underlying(123) returns Future.value(111)
      underlying(321) returns Future.value(222)

      promise() = Return(underlying)

      f123.isDefined must beTrue
      f321.isDefined must beTrue

      f123() must be_==(111)
      f321() must be_==(222)
    }

    "fail requests when underlying service provision fails" in {
      val promise = new Promise[Service[Int, Int]]
      val proxy = new ProxyService(promise)

      val f = proxy(123)

      promise() = Throw(new Exception("sad panda"))

      f.isDefined must beTrue
      f() must throwA(new Exception("sad panda"))
    }

    "proxy cancellation" in {
      val promise = new Promise[Service[Int, Int]]
      val proxy = new ProxyService(promise)

      val f123 = proxy(123)

      val replyPromise = new Promise[Int]
      underlying(123) returns replyPromise

      f123.cancel()

      promise() = Return(underlying)

      f123.isDefined must beTrue
      replyPromise.isDefined must beFalse
      f123() must throwA(new CancelledConnectionException)
    }
  }
}
