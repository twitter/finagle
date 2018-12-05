package com.twitter.finagle.http2.exp.transport

import com.twitter.finagle.Stack
import com.twitter.finagle.http2.transport.Http2UpgradingTransport
import com.twitter.finagle.param.Stats
import com.twitter.finagle.stats.InMemoryStatsReceiver
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http.HttpClientUpgradeHandler.UpgradeEvent
import io.netty.handler.codec.http.{
  DefaultFullHttpRequest,
  HttpClientCodec,
  HttpClientUpgradeHandler,
  HttpMethod,
  HttpVersion
}
import org.scalatest.FunSuite
import scala.collection.mutable

class UpgradeRequestHandlerTest extends FunSuite {

  private class Ctx {
    val stats = new InMemoryStatsReceiver
    val params = Stack.Params.empty + Stats(stats)
    val clientCodec = new HttpClientCodec()
    val upgradeRequestHandler = new UpgradeRequestHandler(params, clientCodec)

    val events = new mutable.Queue[Any]
    val eventCatcher = new ChannelInboundHandlerAdapter {
      override def userEventTriggered(ctx: ChannelHandlerContext, evt: scala.Any): Unit =
        events += evt
    }

    val channel = new EmbeddedChannel(clientCodec, upgradeRequestHandler, eventCatcher)
  }

  test("full http-requests without a body add the upgrade machinery") {
    new Ctx {
      val request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
      channel.writeOutbound(request)

      assert(stats.counters.get(Seq("upgrade", "attempt")) == Some(1l))
      assert(channel.pipeline.get(classOf[HttpClientUpgradeHandler]) != null)
    }
  }

  test("full http-requests with a body don't add") {
    new Ctx {
      val request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
      request.content().writeByte(1)
      channel.writeOutbound(request)

      assert(stats.counters.get(Seq("upgrade", "ignored")) == Some(1l))
      assert(channel.pipeline.get(classOf[HttpClientUpgradeHandler]) == null)
      assert(channel.pipeline.get(classOf[UpgradeRequestHandler]) == null)
    }
  }

  test("UPGRADE_ISSUED events are swallowed") {
    new Ctx {
      val request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
      channel.writeOutbound(request)

      // Should be swallowed
      channel.pipeline.fireUserEventTriggered(UpgradeEvent.UPGRADE_ISSUED)
      assert(events.isEmpty)
      assert(channel.pipeline.get(classOf[UpgradeRequestHandler]) == upgradeRequestHandler)
    }
  }

  test(
    "UPGRADE_REJECTED events are converted and " +
      "the handler removed from the pipeline"
  ) {
    new Ctx {
      val request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
      channel.writeOutbound(request)

      channel.pipeline.fireUserEventTriggered(UpgradeEvent.UPGRADE_REJECTED)
      assert(events.isEmpty)
      assert(channel.readInbound[Object]() == Http2UpgradingTransport.UpgradeRejected)
      // We should have been removed.
      assert(channel.pipeline.get(classOf[UpgradeRequestHandler]) == null)
    }
  }

  test("random events pass through") {
    new Ctx {
      val request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
      channel.writeOutbound(request)

      // This one should pass through
      val evt = new Object
      channel.pipeline.fireUserEventTriggered(evt)
      assert(events.dequeue() == evt)
      assert(channel.pipeline.get(classOf[UpgradeRequestHandler]) == upgradeRequestHandler)
    }
  }
}
