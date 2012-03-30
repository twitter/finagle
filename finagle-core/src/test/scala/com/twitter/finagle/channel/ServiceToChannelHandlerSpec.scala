package com.twitter.finagle.channel

import java.util.logging.{Logger, Level}

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import org.mockito.{Matchers, ArgumentCaptor}

import java.net.InetSocketAddress
import org.jboss.netty.channel.{
  ChannelHandlerContext, MessageEvent, Channel,
  ChannelPipeline, DownstreamMessageEvent,
  ChannelStateEvent, Channels, ExceptionEvent}

import com.twitter.util.{Future, Promise, Return, NullMonitor}
import com.twitter.finagle.{ClientConnection, Service, ServiceFactory}
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver, InMemoryStatsReceiver}

class ServiceToChannelHandlerSpec extends SpecificationWithJUnit with Mockito {
  "ServiceToChannelHandler" should {
    class Foo { def fooMethod() = "hey there" }

    val statsReceiver = new InMemoryStatsReceiver
    val log = mock[Logger]
    val request = new Foo
    val service = mock[Service[Foo, String]]
    val serviceFactory = mock[ServiceFactory[Foo, String]]
    serviceFactory(any) returns Future.value(service)
    val handler = new ServiceToChannelHandler(
      serviceFactory, statsReceiver, log, NullMonitor, true)
    val pipeline = mock[ChannelPipeline]
    val channel = mock[Channel]
    val closeFuture = Channels.future(channel)
    channel.close returns closeFuture
    channel.isOpen returns true
    channel.getCloseFuture returns closeFuture
    val address = mock[InetSocketAddress]
    address.toString returns "ADDRESS"
    channel.getRemoteAddress returns address
    val ctx = mock[ChannelHandlerContext]
    channel.getPipeline returns pipeline
    ctx.getChannel returns channel
    val e = mock[MessageEvent]
    e.getMessage returns request

    // This opens the channel, so that the ClosingHandler discovers
    // the channel.
    handler.channelOpen(ctx, mock[ChannelStateEvent])

    "when sending a valid message" in {
      service(Matchers.any[Foo]) answers { f => Future.value(f.asInstanceOf[Foo].fooMethod) }
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
      service(Matchers.any[Foo]) answers { f => Future.value(f.asInstanceOf[Foo].fooMethod) }
      "when sending an invalid message" in {
        e.getMessage returns mock[Object]   // wrong type
        handler.messageReceived(ctx, e)

        // Unfortunately, we rely on catching the ClassCastError from
        // the invocation of the service itself :-/
        //   there was no(service)(Matchers.any[Foo])
        there was one(service).release()
        there was one(channel).close()

        there was one(log).log(
          Matchers.eq(Level.SEVERE),
          Matchers.eq("A Service threw an exception"),
          any[ClassCastException])
      }

      "an exception was caught by Netty" in {
        val exc = new Exception("netty exception")
        val e = mock[ExceptionEvent]
        e.getCause returns exc
        handler.exceptionCaught(mock[ChannelHandlerContext], e)
        there was one(service).release()
        there was one(channel).close()
        there was one(log).log(Level.WARNING, "Unhandled exception in connection with ADDRESS , shutting down connection", exc)

      }

      "a close exception was caught by Netty" in {
        val exc = new java.nio.channels.ClosedChannelException
        val e = mock[ExceptionEvent]
        e.getCause returns exc
        handler.exceptionCaught(mock[ChannelHandlerContext], e)
        there was one(service).release()
        there was one(channel).close()
        there was no(log).log(Level.WARNING, "Unhandled exception in connection with ADDRESS , shutting down connection", exc)
        there was one(log).log(Level.FINEST, "Unhandled exception in connection with ADDRESS , shutting down connection", exc)
      }

      "when the service handler throws (encoded)" in {
        val exc = new Exception("WTF")
        service(request) returns Future.exception(exc)
        handler.messageReceived(ctx, e)

        there was one(service).release()
        there was one(channel).close()

        there was one(log).log(Level.SEVERE, "A Service threw an exception", exc)
      }

      "when the service handler throws (raw)" in {
        val exc = new RuntimeException("WTF")
        service(request) throws exc
        handler.messageReceived(ctx, e) mustNot throwA[Throwable]

        there was one(service).release()
        there was one(channel).close()
        there was one(log).log(Level.SEVERE, "A Service threw an exception", exc)
      }

      "when the service handlers throws (indirect)" in {
        val exc = new Exception("indirect exception")
        val res = new Promise[String]
        val inner = new Promise[String]
        service(request) answers { _ =>
          inner ensure { throw exc }
          res
        }
        handler.messageReceived(ctx, e)
        there was one(service)(request)
        inner() = Return("ok")

        there was one(service).release()
        there was one(channel).close()
        there was one(log).log(Level.SEVERE, "A Service threw an exception", exc)
      }
    }

    "on close" in {
      "pending request is cancelled (hangupOnCancel=true)" in {
        val p = new Promise[String]
        service(any) returns p
        handler.messageReceived(ctx, e)
        there was one(service)(request)
        val exc = new Exception("netty exception")
        val ee = mock[ExceptionEvent]
        ee.getCause returns exc
        p.isCancelled must beFalse
        statsReceiver.counters mustNot haveKey(Seq("shutdown_while_pending"))
        handler.exceptionCaught(mock[ChannelHandlerContext], ee)
        p.isCancelled must beTrue
        there was one(log).log(Level.WARNING, "Unhandled exception in connection with ADDRESS , shutting down connection", exc)
        there was no(pipeline).sendDownstream(any)
        p() = Return("ok")
        there was no(pipeline).sendDownstream(any)
        statsReceiver.counters must haveKey(Seq("shutdown_while_pending"))
        statsReceiver.counters(Seq("shutdown_while_pending")) must be_==(1)
      }

      "pending request ISN'T cancelled (hangupOnCancel=false)" in {
        val handler = new ServiceToChannelHandler(serviceFactory, statsReceiver, log, NullMonitor, false)
        handler.channelOpen(ctx, mock[ChannelStateEvent])
        val p = new Promise[String]
        service(any) returns p
        handler.messageReceived(ctx, e)
        there was one(service)(request)
        val exc = new Exception("netty exception")
        val ee = mock[ExceptionEvent]
        ee.getCause returns exc
        p.isCancelled must beFalse
        handler.exceptionCaught(mock[ChannelHandlerContext], ee)
        p.isCancelled must beFalse
      }
    }
  }
}
