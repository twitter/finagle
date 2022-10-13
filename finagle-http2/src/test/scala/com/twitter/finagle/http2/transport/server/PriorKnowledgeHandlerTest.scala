package com.twitter.finagle.http2.transport.server

import com.twitter.finagle.Stack.Params
import com.twitter.finagle.netty4.http._
import com.twitter.finagle.netty4.http.handler.UriValidatorHandler
import com.twitter.finagle.param.Stats
import com.twitter.finagle.stats.InMemoryStatsReceiver
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufUtil
import io.netty.buffer.Unpooled._
import io.netty.channel._
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http2.Http2CodecUtil._
import io.netty.util.CharsetUtil._
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

class PriorKnowledgeHandlerTest extends AnyFunSuite with BeforeAndAfter with MockitoSugar {
  // We need to extend ChannelInboundHandlerAdapter because without doing so, since all
  // methods are marked @Skip, the handler will be skipped altogether
  class TestHandler extends ChannelInboundHandlerAdapter {
    override def channelRead(
      ctx: ChannelHandlerContext,
      msg: Any
    ): Unit = ctx.fireChannelRead(msg)
  }

  val PriorKnowledgeHandlerName = "priorKnowledgeHandler"

  var pipeline: ChannelPipeline = null
  var channel: EmbeddedChannel = null
  var mockHandler: ChannelInboundHandlerAdapter = null

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

    mockHandler = mock[TestHandler]
    pipeline.addLast(mockHandler)

    // we place this after the mockhandler so even when replaced with an http/2 codec
    // the mockhandler still sees messages first
    val http11Codec = new ChannelHandlerAdapter() {}
    pipeline.addLast(HttpCodecName, http11Codec)
    val upgradeHandler = new ChannelHandlerAdapter() {}
    pipeline.addLast("upgradeHandler", upgradeHandler)
    pipeline.addLast(UriValidatorHandler.HandlerName, UriValidatorHandler)
  }

  test("removes self and re-emits consumed bytes when not matching") {
    val nonPrefaceBytes = directBuffer(24)
      .writeBytes(
        "PRI * HTTP/18 Foo\nBAR AB"
          .getBytes(UTF_8)
      )
      .asReadOnly()

    channel.writeInbound(nonPrefaceBytes)

    verify(mockHandler).channelRead(anyObject(), ArgumentMatchers.eq(nonPrefaceBytes))
    assert(pipeline.names().contains(HttpCodecName))
    assert(pipeline.names().contains(UriValidatorHandler.HandlerName))
    assert(!pipeline.names().contains(PriorKnowledgeHandlerName))
    assert(!pipeline.names().contains(H2UriValidatorHandler.HandlerName))
    assert(stats.counters(Seq("upgrade", "success")) == 0)
  }

  test("partial matching buffers are sent down pipeline on no match") {
    val partialPreface = connectionPrefaceBuf.slice(0, 10)
    val nonMatching = directBuffer(16)
      .writeBytes(
        "MORE BYTES\n\nHERE"
          .getBytes(UTF_8)
      )
      .asReadOnly()

    channel.writeInbound(partialPreface.duplicate())

    verify(mockHandler, never()).channelRead(anyObject(), anyObject())

    channel.writeInbound(nonMatching)

    val msgCapture: ArgumentCaptor[ByteBuf] = ArgumentCaptor.forClass(classOf[ByteBuf])
    verify(mockHandler, times(2)).channelRead(anyObject(), msgCapture.capture())

    assert(pipeline.names().contains(HttpCodecName))
    assert(pipeline.names().contains(UriValidatorHandler.HandlerName))
    assert(!pipeline.names().contains(PriorKnowledgeHandlerName))

    val capturedMessages = msgCapture.getAllValues

    assert(capturedMessages.size() == 2)

    assert(ByteBufUtil.equals(capturedMessages.get(0), partialPreface))

    assert(ByteBufUtil.equals(capturedMessages.get(1), nonMatching))

    assert(stats.counters(Seq("upgrade", "success")) == 0)
  }

  test("removes self replaces http with http/2 and re-emits bytes when matching") {
    // append extra bytes after preface to ensure that everything is correctly passed along
    val extraBytes = directBuffer(16)
      .writeBytes(
        "MORE BYTES\n\nHERE"
          .getBytes(UTF_8)
      )
      .asReadOnly()

    val prefacePlusExtra =
      directBuffer(40).writeBytes(connectionPrefaceBuf).writeBytes(extraBytes.duplicate())

    channel.writeInbound(prefacePlusExtra)

    val msgCapture: ArgumentCaptor[ByteBuf] = ArgumentCaptor.forClass(classOf[ByteBuf])
    verify(mockHandler, times(2)).channelRead(anyObject(), msgCapture.capture())
    assert(pipeline.names().contains(Http2CodecName))
    assert(!pipeline.names().contains(HttpCodecName))
    assert(!pipeline.names().contains(PriorKnowledgeHandlerName))
    assert(!pipeline.names().contains(UriValidatorHandler.HandlerName))

    val capturedMessages = msgCapture.getAllValues

    assert(capturedMessages.size() == 2)

    assert(ByteBufUtil.equals(capturedMessages.get(0), connectionPrefaceBuf))

    assert(ByteBufUtil.equals(capturedMessages.get(1), extraBytes))

    assert(stats.counters(Seq("upgrade", "success")) == 1)
  }
}
