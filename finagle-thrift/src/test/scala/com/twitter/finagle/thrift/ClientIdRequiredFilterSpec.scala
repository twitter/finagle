package com.twitter.finagle.thrift

import com.twitter.finagle.{Service, Filter}
import com.twitter.util.{Future, Local, Return, Throw}
import org.specs.mock.Mockito
import org.specs.Specification

object ClientIdRequiredFilterSpec extends Specification with Mockito {
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
        result() mustEqual response()
        result
      }
    }

    "throws NoClientIdSpecifiedException when ClientId does not exist" in {
      service(request)() must throwA[NoClientIdSpecifiedException]
      there was no(underlying).apply(any[String])
    }
  }
}
