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
class ClientIdRequiredFilterTest extends FunSuite with MockitoSugar {

  case class ClientIdRequiredFilterContext(underlying: Service[String,String]) {
    lazy val service = new ClientIdRequiredFilter andThen underlying
  }

  val request = "request"
  val response = Future.value("response")
  val clientId = ClientId("test")
  
  test("ClientIdRequiredFilter passes through when ClientId exists") {
    val c = ClientIdRequiredFilterContext(mock[Service[String,String]])
    import c._

    when(underlying(request)).thenReturn(response)
    clientId.asCurrent {
      val result = service(request)
      assert(Await.result(result) == Await.result(response))
      result
    }
  }

  test("ClientIdRequiredFilter throws NoClientIdSpecifiedException when ClientId does not exist") {
    val c = ClientIdRequiredFilterContext(mock[Service[String,String]])
    import c._

    ClientId.let(None) {
      intercept[NoClientIdSpecifiedException]{
        Await.result(service(request))
      }
      verify(underlying, times(0)).apply(Matchers.anyString())
    }
  }
}
