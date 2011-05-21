package com.twitter.finagle.channel

import java.util.logging.Logger

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.{Matchers, ArgumentCaptor}

import org.jboss.netty.channel.{
  ChannelHandlerContext, MessageEvent, Channel,
  ChannelPipeline, DownstreamMessageEvent,
  ChannelStateEvent, Channels}

import com.twitter.util.Future
import com.twitter.finagle.{ClientConnection, PostponedService, Service}
import com.twitter.finagle.stats.StatsReceiver

object ServiceToChannelHandlerSpec extends Specification with Mockito {
  "ServiceToChannelHandler" should {
    class Foo { def fooMethod() = "hey there" }

    val log = mock[StatsReceiver]
    val request = new Foo
    val service = mock[Service[Foo, String]]
    val postponedService = mock[PostponedService[Foo, String]]
    val serviceFactory = { (clientConnection: ClientConnection) => service }
    val handler = new ServiceToChannelHandler(service, postponedService, serviceFactory,
      log, Logger.getLogger(getClass.getName))
    val pipeline = mock[ChannelPipeline]
    val channel = mock[Channel]
    val closeFuture = Channels.future(channel)
    channel.close returns closeFuture
    channel.isOpen returns true
    val ctx = mock[ChannelHandlerContext]
    channel.getPipeline returns pipeline
    ctx.getChannel returns channel
    val e = mock[MessageEvent]
    e.getMessage returns request

    // This opens the channel, so that the ClosingHandler discovers
    // the channel.
    handler.channelOpen(ctx, mock[ChannelStateEvent])

    service(Matchers.any[Foo]) answers { f => Future.value(f.asInstanceOf[Foo].fooMethod) }

    "when sending a valid message" in {
      handler.messageReceived(ctx, e)

      "propagate received messages to the service" in {
        there was one(service)(request)
      }

      "write the reply to the channel" in {
        val captor = ArgumentCaptor.forClass(classOf[DownstreamMessageEvent])
        there was one(pipeline).sendDownstream(captor.capture)

        val dsme = captor.getValue
        dsme.getMessage must haveClass[String]
        dsme.getMessage must be_==("hey there")
      }
    }

    "service shutdown" in {
      "when sending an invalid message" in {
        e.getMessage returns mock[Object]   // wrong type
        handler.messageReceived(ctx, e)

        // Unfortunately, we rely on catching the ClassCastError from
        // the invocation of the service itself :-/
        //   there was no(service)(Matchers.any[Foo])
        there was one(service).release()
        there was one(channel).close()
      }

      "when the service handler throws" in {
        service(request) returns Future.exception(new Exception("WTF"))
        handler.messageReceived(ctx, e)

        there was one(service).release()
        there was one(channel).close()
      }
    }
  }
}
