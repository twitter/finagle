package com.twitter.finagle.http2.transport

import com.twitter.finagle.netty4.http.exp.HttpCodecName
import com.twitter.finagle.param.Stats
import com.twitter.finagle.Stack.Params
import com.twitter.finagle.stats.InMemoryStatsReceiver
import io.netty.channel._
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.ssl.{SslHandler, SslHandshakeCompletionEvent, ApplicationProtocolNames}
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class NpnOrAlpnHandlerTest extends FunSuite with BeforeAndAfter with MockitoSugar {

  val http2 = ApplicationProtocolNames.HTTP_2
  val http11 = ApplicationProtocolNames.HTTP_1_1

  var pipeline: ChannelPipeline = null
  var sslHandler: SslHandler = null

  val stats = new InMemoryStatsReceiver
  var params = Params.empty + Stats(stats)

  before {
    stats.clear()
    val channel = new EmbeddedChannel()
    pipeline = channel.pipeline()

    val init = new ChannelInitializer[Channel] {
      def initChannel(ch: Channel): Unit = {}
    }

    sslHandler = mock[SslHandler]
    doCallRealMethod().when(sslHandler).userEventTriggered(any[ChannelHandlerContext], anyObject())
    pipeline.addLast(sslHandler)

    val handler = new NpnOrAlpnHandler(init, params)
    pipeline.addLast(handler)

    val dummyHttp11Codec = new ChannelHandlerAdapter() {}
    pipeline.addLast(HttpCodecName, dummyHttp11Codec)
  }

  test("Replaces http codec with http/2 codec when h2 negotiated & records stat") {
    when(sslHandler.applicationProtocol()).thenReturn(http2)
    pipeline.fireUserEventTriggered(SslHandshakeCompletionEvent.SUCCESS)
    assert(!pipeline.names().contains(HttpCodecName))
    assert(pipeline.names().contains("Http2FrameCodec#0"))
    assert(stats.counters(Seq("upgrade", "success")) == 1)
  }

  test("Leaves http codec in place when http/1.1 is negotiated & doesn't record stat") {
    when(sslHandler.applicationProtocol()).thenReturn(http11)
    pipeline.fireUserEventTriggered(SslHandshakeCompletionEvent.SUCCESS)
    assert(pipeline.names().contains(HttpCodecName))
    assert(!pipeline.names().contains("http2Codec"))
    assert(!stats.counters.contains(Seq("upgrade", "success")))
  }
}
