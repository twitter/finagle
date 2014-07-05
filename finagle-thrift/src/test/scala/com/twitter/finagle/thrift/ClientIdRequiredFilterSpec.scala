package com.twitter.finagle.thrift

import com.twitter.finagle.Service
import com.twitter.util.{Await, Future}
import org.junit.runner.RunWith
import org.mockito.Matchers
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class ClientIdRequiredFilterSpec extends FunSuite with MockitoSugar {

  val request = "request"
  val response = Future.value("response")
  val clientId = ClientId("test")
  test("ClientIdRequiredFilter passes through when ClientId exists") {
    val underlying = mock[Service[String, String]]
    val service = new ClientIdRequiredFilter andThen underlying

    when(underlying(request)).thenReturn(response)
    clientId.asCurrent {
      val result = service(request)
      assert(Await.result(result) === Await.result(response))
      result
    }
  }

  test("ClientIdRequiredFilter throws NoClientIdSpecifiedException when ClientId does not exist") {
    val underlying = mock[Service[String, String]]
    val service = new ClientIdRequiredFilter andThen underlying

    ClientId.clear()
    intercept[NoClientIdSpecifiedException]{
      Await.result(service(request))
    }
    verify(underlying, times(0)).apply(Matchers.anyString())
  }
}
