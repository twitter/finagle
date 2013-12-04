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
    val duration = 4.seconds
    val bypassedClientPrefix = "bypassMe"
    val filter = new ThriftForwardingWarmUpFilter(
      duration,
      forwardService,
      isBypassClient = { _.name.startsWith("bypassMe") }
    )
    val req = new Array[Byte](1)
    val rep = new Array[Byte](2)
  }

  test("forward all at time zero") {
    Time.withCurrentTimeFrozen { ctl =>
      val ctx = newCtx()
      import ctx._

      when(forwardService(any[ThriftClientRequest])) thenReturn Future.value(rep)
      filter(req, service)
      0 until 9 foreach { _ => filter(req, service) }
      verify(forwardService, times(10))(any[ThriftClientRequest])
    }
  }

  test("forward none once passed duration") {
    Time.withCurrentTimeFrozen { ctl =>
      val ctx = newCtx()
      import ctx._

      when(forwardService(any[ThriftClientRequest])) thenReturn Future.value(rep)
      when(service(any[Array[Byte]])) thenReturn Future.value(rep)
      filter(req, service)
      ctl.advance(duration)
      0 until 9 foreach { _ => filter(req, service) }
      verify(forwardService, times(1))(any[ThriftClientRequest])
      verify(service, times(9))(any[Array[Byte]])
    }
  }

  test("don't forward for bypassed clients") {
    ClientId("bypassMe.prod").asCurrent {
      Time.withCurrentTimeFrozen { ctl =>
        val ctx = newCtx()
        import ctx._

        when(service(any[Array[Byte]])) thenReturn Future.value(rep)
        0 until 9 foreach { _ => filter(req, service) }
        verify(service, times(9))(any[Array[Byte]])
      }
    }
  }
}
