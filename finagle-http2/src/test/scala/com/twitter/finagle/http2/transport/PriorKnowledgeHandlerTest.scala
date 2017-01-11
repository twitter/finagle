package com.twitter.finagle.http2.transport

import com.twitter.finagle.netty4.http.exp._
import com.twitter.finagle.param.Stats
import com.twitter.finagle.Stack.Params
import com.twitter.finagle.stats.InMemoryStatsReceiver
import io.netty.buffer.Unpooled._
import io.netty.buffer.{ByteBufUtil, ByteBuf}
import io.netty.channel._
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http2.Http2CodecUtil._
import io.netty.util.CharsetUtil._
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.{ArgumentCaptor, Matchers}
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class PriorKnowledgeHandlerTest extends FunSuite with BeforeAndAfter with MockitoSugar {

  val PriorKnowledgeHandlerName = "priorKnowledgeHandler"

  var pipeline: ChannelPipeline = null
  var channel: EmbeddedChannel = null
  var dummyHandler: ChannelInboundHandlerAdapter = null

  val stats = new InMemoryStatsReceiver
  var params = Params.empty + Stats(stats)

  before {
    stats.clear()
    channel = new EmbeddedChannel()
    pipeline = channel.pipeline()

    val initializer = new ChannelInitializer[Channel] {
      def initChannel(ch: Channel): Unit = {}
    }

    val priorKnowledgeHandler = new PriorKnowledgeHandler(initializer, params)
    pipeline.addLast(PriorKnowledgeHandlerName, priorKnowledgeHandler)

    dummyHandler = mock[ChannelInboundHandlerAdapter]
    pipeline.addLast(dummyHandler)

    // we place this after the dummy handler so even when replaced with an http/2 codec
    // the dummy handler still sees messages first
    val dummyHttp11Codec = new ChannelHandlerAdapter() {}
    pipeline.addLast(HttpCodecName, dummyHttp11Codec)
    val dummyUpgradeHandler = new ChannelHandlerAdapter() {}
    pipeline.addLast("upgradeHandler", dummyUpgradeHandler)
  }


  test("removes self and re-emits consumed bytes when not matching") {
    val nonPrefaceBytes = directBuffer(24).writeBytes("PRI * HTTP/18 Foo\nBAR AB"
      .getBytes(UTF_8)).asReadOnly()

    channel.writeInbound(nonPrefaceBytes)

    verify(dummyHandler).channelRead(anyObject(), Matchers.eq(nonPrefaceBytes))
    assert(!pipeline.names().contains(PriorKnowledgeHandlerName))
    assert(pipeline.names().contains(HttpCodecName))
    assert(!stats.counters.contains(Seq("upgrade", "success")))
  }


  test("partial matching buffers are sent down pipeline on no match") {
    val partialPreface = connectionPrefaceBuf.slice(0, 10)
    val nonMatching = directBuffer(16).writeBytes("MORE BYTES\n\nHERE"
      .getBytes(UTF_8)).asReadOnly()

    channel.writeInbound(partialPreface)

    verify(dummyHandler, never()).channelRead(anyObject(), anyObject())

    channel.writeInbound(nonMatching)

    val msgCapture: ArgumentCaptor[ByteBuf] = ArgumentCaptor.forClass(classOf[ByteBuf])
    verify(dummyHandler, times(2)).channelRead(anyObject(), msgCapture.capture())

    assert(!pipeline.names().contains(PriorKnowledgeHandlerName))
    assert(pipeline.names().contains(HttpCodecName))

    val capturedMessages = msgCapture.getAllValues

    assert(capturedMessages.size() == 2)

    assert(ByteBufUtil.equals(capturedMessages.get(0), 0,
      partialPreface, 0, partialPreface.capacity()))

    assert(ByteBufUtil.equals(capturedMessages.get(1), 0,
      nonMatching, 0, nonMatching.capacity()))

    assert(!stats.counters.contains(Seq("upgrade", "success")))
  }


  test("removes self replaces http with http/2 and re-emits bytes when matching") {
    // append extra bytes after preface to ensure that everything is correctly passed along
    val extraBytes = directBuffer(16).writeBytes("MORE BYTES\n\nHERE"
      .getBytes(UTF_8)).asReadOnly()

    val prefacePlusExtra = directBuffer(40).writeBytes(connectionPrefaceBuf).writeBytes(extraBytes)

    channel.writeInbound(prefacePlusExtra)

    val msgCapture: ArgumentCaptor[ByteBuf] = ArgumentCaptor.forClass(classOf[ByteBuf])
    verify(dummyHandler, times(2)).channelRead(anyObject(), msgCapture.capture())
    assert(!pipeline.names().contains(PriorKnowledgeHandlerName))
    assert(!pipeline.names().contains(HttpCodecName))
    assert(!pipeline.names().contains("http2Codec"))

    val capturedMessages = msgCapture.getAllValues

    assert(capturedMessages.size() == 2)

    assert(ByteBufUtil.equals(capturedMessages.get(0), 0,
      connectionPrefaceBuf, 0, connectionPrefaceBuf.capacity()))

    assert(ByteBufUtil.equals(capturedMessages.get(1), 0,
      extraBytes, 0, extraBytes.capacity()))

    assert(stats.counters(Seq("upgrade", "success")) == 1)
  }
}
