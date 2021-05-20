package com.twitter.finagle.http2.transport.client

import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.http2.MultiplexHandlerBuilder
import com.twitter.finagle.{Service, Stack}
import com.twitter.finagle.http2.transport.client.UpgradeRequestHandler.HandlerName
import com.twitter.finagle.param.Stats
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.Promise
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.handler.codec.http.HttpClientUpgradeHandler.UpgradeEvent
import io.netty.handler.codec.http.{
  DefaultFullHttpRequest,
  HttpClientCodec,
  HttpClientUpgradeHandler,
  HttpMethod,
  HttpVersion
}
import scala.collection.mutable
import org.scalatest.funsuite.AnyFunSuite

class UpgradeRequestHandlerTest extends AnyFunSuite {

  private class Ctx {
    val stats = new InMemoryStatsReceiver
    val params = Stack.Params.empty + Stats(stats)
    val clientCodec = new HttpClientCodec()

    val fOnH2Service = Promise[Service[Request, Response]]()

    private[this] def onH2Service(h2Service: Service[Request, Response]): Unit =
      fOnH2Service.setValue(h2Service)

    val upgradeRequestHandler =
      new UpgradeRequestHandler(params, onH2Service, clientCodec, identity(_))

    val events = new mutable.Queue[Any]
    val eventCatcher = new ChannelInboundHandlerAdapter {
      override def userEventTriggered(ctx: ChannelHandlerContext, evt: scala.Any): Unit =
        events += evt
    }

    val channel: EmbeddedChannel = {
      val ch = new EmbeddedChannel()
      ch.pipeline
        .addLast(clientCodec)
        .addLast(HandlerName, upgradeRequestHandler)
        .addLast(eventCatcher)
      ch
    }
  }

  test("full http-requests without a body add the upgrade machinery") {
    new Ctx {
      val request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
      channel.writeOutbound(request)

      assert(stats.counters.get(Seq("upgrade", "attempt")) == Some(1L))
      assert(channel.pipeline.get(classOf[HttpClientUpgradeHandler]) != null)
    }
  }

  test("upgrades offer an `OnH2Session`") {
    new Ctx {
      val childChannel = new EmbeddedChannel()
      val parentCtx = channel.pipeline.context(classOf[UpgradeRequestHandler])

      // Need to add the Htt2FrameCodec so our ClientSessionImpl doesn't blow up.
      // It has to be added at the front because otherwise it will emit messages
      // into the `UpgradeRequestHandler`.
      val (codec, _) = MultiplexHandlerBuilder.clientFrameCodec(params, None)
      channel.pipeline.addFirst(codec)
      upgradeRequestHandler.initializeUpgradeStreamChannel(childChannel, parentCtx)
      assert(fOnH2Service.poll.isDefined)
    }
  }

  test("full http-requests with a body don't add") {
    new Ctx {
      val request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
      request.content().writeByte(1)
      channel.writeOutbound(request)

      assert(stats.counters.get(Seq("upgrade", "ignored")) == Some(1L))
      assert(channel.readInbound[Object]() == Http2UpgradingTransport.UpgradeAborted)
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
