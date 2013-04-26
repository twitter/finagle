package com.twitter.finagle.thrift

import com.twitter.finagle.Service
import com.twitter.util.{Await, Future}
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class ClientIdRequiredFilterSpec extends SpecificationWithJUnit with Mockito {
  "ClientIdRequiredFilter" should {
    val underlying = mock[Service[String, String]]

    val service = new ClientIdRequiredFilter andThen underlying
    val request = "request"
    val response = Future.value("response")
    val clientId = ClientId("test")
    "passes through when ClientId exists" in {
      underlying(request) returns response
      clientId.asCurrent {
        val result = service(request)
        Await.result(result) mustEqual Await.result(response)
        result
      }
    }

    "throws NoClientIdSpecifiedException when ClientId does not exist" in {
      Await.result(service(request)) must throwA[NoClientIdSpecifiedException]
      there was no(underlying).apply(any[String])
    }
  }
}
