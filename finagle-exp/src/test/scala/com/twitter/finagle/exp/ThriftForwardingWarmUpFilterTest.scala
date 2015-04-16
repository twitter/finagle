package com.twitter.finagle.exp

import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.thrift.{ClientId, ThriftClientRequest}
import com.twitter.util.{Future, Time}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class ThriftForwardingWarmUpFilterTest extends FunSuite with MockitoSugar {

  def newCtx() = new {
    val service = mock[Service[Array[Byte], Array[Byte]]]
    val forwardService = mock[Service[ThriftClientRequest, Array[Byte]]]
    val numRequests = 9
    val duration = 4.seconds
    val bypassedClientPrefix = "bypassMe"
    val filter = new ThriftForwardingWarmUpFilter(
      duration,
      forwardService,
      isBypassClient = { _.name.startsWith(bypassedClientPrefix) }
    )
    val req = new Array[Byte](1)
    val rep = new Array[Byte](2)

    def mockService[A](service: Service[A, Array[Byte]]) =
      when(service(any[A])) thenReturn Future.value(rep)
    def sendRequests() = 0 until numRequests foreach { _ => filter(req, service) }
  }

  test("forward all at time zero") {
    ClientId("someClient.prod").asCurrent {
      Time.withCurrentTimeFrozen { ctl =>
        val ctx = newCtx()
        import ctx._

        mockService(forwardService)
        filter(req, service)
        sendRequests()
        verify(forwardService, times(numRequests + 1))(any[ThriftClientRequest])
      }
    }
  }

  test("forward none once passed duration") {
    ClientId("someClient.prod").asCurrent {
      Time.withCurrentTimeFrozen { ctl =>
        val ctx = newCtx()
        import ctx._

        mockService(forwardService)
        mockService(service)
        filter(req, service)
        ctl.advance(duration)
        sendRequests()
        verify(forwardService, times(1))(any[ThriftClientRequest])
        verify(service, times(numRequests))(any[Array[Byte]])
      }
    }
  }

  test("don't forward for bypassed clients") {
    ClientId("bypassMe.prod").asCurrent {
      Time.withCurrentTimeFrozen { ctl =>
        val ctx = newCtx()
        import ctx._

        mockService(service)
        sendRequests()
        verify(service, times(numRequests))(any[Array[Byte]])
      }
    }
  }

  test("don't forward for unidentified clients") {
    Time.withCurrentTimeFrozen { ctl =>
      val ctx = newCtx()
      import ctx._

      mockService(service)
      sendRequests()
      verify(service, times(numRequests))(any[Array[Byte]])
    }
  }
}
