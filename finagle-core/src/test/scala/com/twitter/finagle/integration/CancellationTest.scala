package com.twitter.finagle.integration

import com.twitter.finagle.{Failure, CancelledConnectionException}
import com.twitter.util.Await
import org.jboss.netty.channel._
import org.mockito.ArgumentCaptor
import org.scalatest.FunSuite
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class CancellationTest extends FunSuite with IntegrationBase with MockitoSugar {
  test("cancel while waiting for a reply") {
    val m = new MockChannel
    val cli = m.build()

    val f = cli("123")
    assert(!f.isDefined)
    m.connectFuture.setSuccess()
    when(m.channel.isOpen) thenReturn true

    assert(!f.isDefined)

    // the request was sent.
    val meCaptor = ArgumentCaptor.forClass(classOf[DownstreamMessageEvent])
    verify(m.channelPipeline).sendDownstream(meCaptor.capture)
    assert(meCaptor.getValue match {
      case event: DownstreamMessageEvent => {
        assert(event.getChannel == m.channel)
        assert(event.getMessage match {
          case s: String => s == "123"
        })
        true
      }
    })

    f.raise(new Exception)
    val seCaptor = ArgumentCaptor.forClass(classOf[DownstreamChannelStateEvent])
    verify(m.channelPipeline, times(2)).sendDownstream(seCaptor.capture)
    assert(seCaptor.getValue match {
      case event: DownstreamChannelStateEvent => {
        assert(event.getChannel == m.channel)
        assert(event.getState == ChannelState.OPEN)
        assert(event.getValue == java.lang.Boolean.FALSE)
        true
      }
    })
  }

  test("cancel while waiting in the queue") {
    val m = new MockChannel
    val client = m.build()
    m.connectFuture.setSuccess()

    verify(m.channelPipeline, times(0)).sendDownstream(any[ChannelEvent])
    val f0 = client("123")
    assert(!f0.isDefined)
    verify(m.channelPipeline).sendDownstream(any[ChannelEvent])
    val f1 = client("333")
    assert(!f1.isDefined)
    verify(m.channelPipeline).sendDownstream(any[ChannelEvent])

    f1.raise(new Exception)
    assert(f1.isDefined)
    val failure = intercept[Failure] {
      Await.result(f1)
    }
    
    assert(failure.getCause.isInstanceOf[CancelledConnectionException])
  }

}
