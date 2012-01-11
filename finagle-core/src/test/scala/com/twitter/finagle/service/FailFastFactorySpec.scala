package com.twitter.finagle.service

import com.twitter.finagle.{WriteException, ServiceFactory, Service}
import com.twitter.util.{Promise, Future}
import java.net.ConnectException
import org.specs.mock.Mockito
import org.specs.Specification

object FailFastFactorySpec extends Specification with Mockito {
  "a FailFastFactory" should {
    val underlyingService = mock[Service[Int, Int]]
    underlyingService.isAvailable returns true

    val underlyingFactory = mock[ServiceFactory[Int, Int]]
    underlyingFactory.isAvailable returns true
    underlyingFactory.make() returns Future.value(underlyingService)

    val factory = new FailFastFactory[Int, Int](underlyingFactory, 1)
    factory.make() returns Future.value(underlyingService)

    "become unavailable if connections failed" in {
      val service = factory.make()()
      factory.isAvailable must beTrue
      service.isAvailable must beTrue

      // Now fail:
      underlyingFactory.make() returns Future.exception(new WriteException(new ConnectException))
      factory.make()
      // factory must be limited but remain available (try to connect for the next request)
      factory.isLimited must beTrue
      factory.isAvailable must beTrue

      val factoryRequest = new Promise[Service[Int,Int]]
      underlyingFactory.make() returns factoryRequest
      factory.make()
      factory.isLimited must beTrue
      // now the factory isn't available because the outstanding connections is == 1
      factory.isAvailable must beFalse

      factoryRequest.setValue(underlyingService)
      factory.isAvailable must beTrue
    }
  }
}
