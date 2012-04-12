package com.twitter.finagle.channel

import com.twitter.finagle._
import com.twitter.finagle.dispatch.{SerialClientDispatcher, ClientDispatcherFactory}
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.util.{Promise, Return, Throw, Future}
import java.net.SocketAddress
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel._
import org.mockito.{Matchers, ArgumentCaptor}
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import scala.collection.JavaConversions._

object ChannelServiceSpec extends SpecificationWithJUnit with Mockito {
  // TODO: we should really mock/drive the dispatcher here. his is
  // Tcurrently more of an integration test.
  def mkDispatcher[Req, Rep]: ClientDispatcherFactory[Req, Rep] =
    (mkTrans) => new SerialClientDispatcher[Req, Rep](mkTrans())

  "ChannelService" should {
    val pipeline = new DefaultChannelPipeline
    val channel = mock[Channel]
    val sink = mock[ChannelSink]
    val closeFuture = Channels.future(channel)
    val factory = mock[ChannelServiceFactory[String, String]]
    val address = mock[SocketAddress]
    channel.getPipeline returns pipeline
    channel.isOpen returns true
    channel.getCloseFuture returns closeFuture
    channel.getRemoteAddress returns address
    pipeline.attach(channel, sink)
    "installs channel handler" in {
      pipeline.toMap.keySet.size() mustEqual 0
      new ChannelService[Any, Any](channel, mkDispatcher, NullStatsReceiver)
      pipeline.toMap.keySet.size() mustEqual 1
    }

    "write requests to the underlying channel" in {
      val service = new ChannelService[String, String](channel, mkDispatcher, NullStatsReceiver)
      val future = service("hello")
      val eventCaptor = ArgumentCaptor.forClass(classOf[ChannelEvent])
      there was one(sink).eventSunk(Matchers.eq(pipeline), eventCaptor.capture)
      eventCaptor.getValue must haveClass[DownstreamMessageEvent]
      val messageEvent = eventCaptor.getValue.asInstanceOf[DownstreamMessageEvent]

      "propagate the correct message" in {
        messageEvent.getMessage must be_==("hello")
      }

      "cause write errors if the downstream write fails" in {
        messageEvent.getFuture.setFailure(new Exception("doh."))
        future() must throwA[WriteException]
      }

      "silently ignore other errors after a downstream write failure" in {
        messageEvent.getFuture.setFailure(new Exception("doh."))
        future() must throwA[WriteException]

        val stateEvent = mock[ChannelStateEvent]
        stateEvent.getState returns ChannelState.OPEN
        stateEvent.getValue returns java.lang.Boolean.FALSE
        val context = mock[ChannelHandlerContext]
        val handler = pipeline.getLast.asInstanceOf[ChannelUpstreamHandler]
        handler.handleUpstream(context, stateEvent)
        // (setting the future twice would cause an exception)
      }
    }

    "propagate cancellation" in {
      val service = new ChannelService[String, String](channel, mkDispatcher, NullStatsReceiver)
      val future = service("hello")

      future.isCancelled must beFalse

      there was one(sink).eventSunk(Matchers.eq(pipeline), any)

      // cancellation causes the channel to close
      future.cancel()
      val eventCaptor = ArgumentCaptor.forClass(classOf[ChannelEvent])
      there were two(sink).eventSunk(Matchers.eq(pipeline), eventCaptor.capture)
      eventCaptor.getValue must beLike {
        case stateEvent: DownstreamChannelStateEvent =>
          stateEvent.getState must be_==(ChannelState.OPEN)
          stateEvent.getValue must be_==(java.lang.Boolean.FALSE)
          true
      }
    }

    "receive replies" in {
      val service = new ChannelService[String, String](channel, mkDispatcher, NullStatsReceiver)
      service.isAvailable must beTrue
      val future = service("hello")
      service.isAvailable must beTrue
      val captor = ArgumentCaptor.forClass(classOf[ChannelEvent])
      there was one(sink).eventSunk(Matchers.eq(pipeline), captor.capture)
      captor.getValue must haveClass[DownstreamMessageEvent]
      val dsme = captor.getValue.asInstanceOf[DownstreamMessageEvent]
      // The serializing dispatcher will not read until the write
      // succeeded.
      dsme.getFuture.setSuccess()

      val handler = pipeline.getLast.asInstanceOf[ChannelUpstreamHandler]
      val context = mock[ChannelHandlerContext]
      val event = mock[MessageEvent]
      event.getMessage returns "olleh"
      future.isDefined must beFalse

      "on success" in {
        handler.handleUpstream(context, event)
        future.isDefined must beTrue
        future() must be_==("olleh")
        service.isAvailable must beTrue
      }

      "on casting error" in {
        event.getMessage returns mock[Object]  // bad type
        handler.handleUpstream(context, event)
        future.isDefined must beTrue
        future() must throwA[ClassCastException]
        // service.isAvailable must beFalse
      }

      "on channel exception" in {
        val exceptionEvent = mock[ExceptionEvent]
        exceptionEvent.getCause returns new Exception("weird")
        handler.handleUpstream(context, exceptionEvent)
        future.isDefined must beTrue
        future() must throwA(new UnknownChannelException(new Exception("weird"), address))

        // The channel was also closed.
        val eventCaptor = ArgumentCaptor.forClass(classOf[ChannelEvent])
        there were two(sink).eventSunk(Matchers.eq(pipeline), eventCaptor.capture)
        eventCaptor.getValue must haveClass[DownstreamChannelStateEvent]
        (eventCaptor.getValue.asInstanceOf[DownstreamChannelStateEvent].getState
         must be_==(ChannelState.OPEN))
        (eventCaptor.getValue.asInstanceOf[DownstreamChannelStateEvent].getValue
         must be_==(java.lang.Boolean.FALSE))

        channel.isOpen returns false
        service.isAvailable must beFalse
      }

      "on channel close" in {
        val stateEvent = mock[ChannelStateEvent]
        stateEvent.getState returns ChannelState.OPEN
        stateEvent.getValue returns java.lang.Boolean.FALSE
        channel.isOpen returns false
        handler.handleUpstream(context, stateEvent)

        future.isDefined must beTrue
        future() must throwA[ChannelClosedException]
        service.isAvailable must beFalse
      }
    }

    "without a request" in {
      val service = new ChannelService[String, String](channel, mkDispatcher, NullStatsReceiver)
      service.isAvailable must beTrue

      "any response is considered spurious" in {
        val handler = pipeline.getLast.asInstanceOf[ChannelUpstreamHandler]
        val context = mock[ChannelHandlerContext]
        val event = mock[MessageEvent]
        event.getMessage returns "hello"
        handler.handleUpstream(context, event)
        service.isAvailable must beTrue
      }
    }
  }

  "ChannelServiceFactory" should {
    val address = mock[SocketAddress]
    val bootstrap = mock[ClientBootstrap]
    val pipeline = new DefaultChannelPipeline
    val pipelineFactory = mock[ChannelPipelineFactory]
    pipelineFactory.getPipeline returns pipeline
    bootstrap.getPipelineFactory returns pipelineFactory
    bootstrap.getOption("remoteAddress") returns address
    val channel = mock[Channel]
    channel.getPipeline returns pipeline
    val channelConfig = mock[ChannelConfig]
    channel.getConfig returns channelConfig
    val closeFuture = new DefaultChannelFuture(channel, false)
    channel.getCloseFuture() returns closeFuture
    channel.close() returns closeFuture
    val channelFactory = mock[ChannelFactory]
    channelFactory.newChannel(any) returns channel
    bootstrap.getFactory returns channelFactory

    val channelFuture = Channels.future(channel)
    channel.connect(address) returns channelFuture

    val factory = new ChannelServiceFactory[Any, Any](bootstrap, mkDispatcher)

    "close the underlying bootstrap on close() with no outstanding requests" in {
      factory.close()
      there was one(bootstrap).releaseExternalResources()
    }

    "close the underlying bootstrap only after all channels are released" in {
      val f = factory()
      there was one(channelFactory).newChannel(pipeline)
      there was one(channel).connect(address)
      f.isDefined must beFalse
      channelFuture.setSuccess()
      f.isDefined must beTrue

      factory.close()
      there was no(bootstrap).releaseExternalResources()
      there was no(channel).close()

      closeFuture.setSuccess()
      there was one(bootstrap).releaseExternalResources()
    }

    "propagate bootstrap errors" in {
      val f = factory()
      f.isDefined must beFalse
      there was one(channel).connect(address)

      channelFuture.setFailure(new Exception("oh crap"))

      f.isDefined must beTrue
      f() must throwA(new WriteException(new Exception("oh crap")))

      // The factory should also be directly closable now.
      factory.close()
      there was one(bootstrap).releaseExternalResources()
    }

    "encode bootstrap exceptions" in {
      val e = new ChannelPipelineException("sad panda")
      channel.connect(address) throws e

      val f = factory()
      f.isDefined must beTrue
      f() must throwA(e)
    }
  }
}
