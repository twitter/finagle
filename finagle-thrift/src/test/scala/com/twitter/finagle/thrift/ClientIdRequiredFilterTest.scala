package com.twitter.finagle.thrift

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Service
import com.twitter.util.Await
import com.twitter.util.Future
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class ClientIdRequiredFilterTest extends AnyFunSuite with MockitoSugar {

  case class ClientIdRequiredFilterContext(underlying: Service[String, String]) {
    lazy val service = new ClientIdRequiredFilter andThen underlying
  }

  val request = "request"
  val response = Future.value("response")
  val clientId = ClientId("test")

  test("ClientIdRequiredFilter passes through when ClientId exists") {
    val c = ClientIdRequiredFilterContext(mock[Service[String, String]])
    import c._

    when(underlying(request)).thenReturn(response)
    clientId.asCurrent {
      val result = service(request)
      assert(Await.result(result, 10.seconds) == Await.result(response, 10.seconds))
      result
    }
  }

  test("ClientIdRequiredFilter throws NoClientIdSpecifiedException when ClientId does not exist") {
    val c = ClientIdRequiredFilterContext(mock[Service[String, String]])
    import c._

    ClientId.let(None) {
      intercept[NoClientIdSpecifiedException] {
        Await.result(service(request), 10.seconds)
      }
      verify(underlying, times(0)).apply(ArgumentMatchers.anyString())
    }
  }
}
