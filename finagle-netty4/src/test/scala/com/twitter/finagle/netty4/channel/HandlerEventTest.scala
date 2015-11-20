package com.twitter.finagle.netty4.channel

import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.Duration
import io.netty.channel._
import io.netty.channel.embedded.{EmbeddedChannel, EmbeddedEventLoop}
import java.net.SocketAddress
import io.netty.channel.local.{LocalEventLoopGroup, LocalChannel}
import io.netty.channel.nio.{NioEventLoopGroup, NioEventLoop}
import io.netty.channel.socket.nio.NioSocketChannel
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar


@RunWith(classOf[JUnitRunner])
class HandlerEventTest extends FunSuite with MockitoSugar {

  // verify that custom channel handlers don't swallow pipeline events.
  val handlers = List(
    new ChannelRequestStatsHandler(new InMemoryStatsReceiver),
    new ChannelStatsHandler(new InMemoryStatsReceiver),
    new SimpleChannelSnooper("test"),
    new ByteBufSnooper("test"),
    new WriteCompletionTimeoutHandler(DefaultTimer.twitter, Duration.fromSeconds(10))
  )
  val loop = new NioEventLoopGroup()

  for (handler <- handlers) testHandler(handler)

  private[this] def testHandler(ch: ChannelHandler): Unit = {
    val handler = new TestDuplexHandler

    val pipeline = new EmbeddedChannel(ch, handler).pipeline
    val name = ch.getClass.getCanonicalName

    // inbound events
    test(s"$name doesn't suppress ChannelActive event") {
      pipeline.fireChannelActive()
      assert(handler.channelActiveFired, "suppressed ChannelActive event")
    }

    test(s"$name doesn't suppress ChannelRead event") {
      pipeline.fireChannelRead(new Object)
      assert(handler.channelReadFired, "suppressed ChannelRead event")
    }

    test(s"$name doesn't suppress ChannelInactive event") {
      pipeline.fireChannelInactive()
      assert(handler.channelInactiveFired, "suppressed ChannelInactive event")
    }

    test(s"$name doesn't suppress ChannelReadComplete event") {
      pipeline.fireChannelReadComplete()
      assert(handler.channelReadCompleteFired, "suppressed ChannelReadComplete event")
    }

    test(s"$name doesn't suppress ChannelRegistered event") {
      pipeline.fireChannelRegistered()
      assert(handler.channelRegisteredFired, "suppressed ChannelRegistered event")
    }

    test(s"$name doesn't suppress ChannelUnregistered event") {
      pipeline.fireChannelUnregistered()
      assert(handler.channelUnregisteredFired, "suppressed ChannelUnregistered event")
    }

    test(s"$name doesn't suppress ChannelWritabilityChanged event") {
      pipeline.fireChannelWritabilityChanged()
      assert(handler.channelWritabilityChangedFired, "suppressed ChannelWritabilityChanged event")
    }

    test(s"$name doesn't suppress ExceptionCaught event") {
      pipeline.fireExceptionCaught(new Exception)
      assert(handler.exceptionCaughtFired, "suppressed ExceptionCaught event")
    }

    test(s"$name doesn't suppress UserEventTriggered event") {
      pipeline.fireUserEventTriggered(new Object)
      assert(handler.userEventTriggeredFired, "suppressed UserEventTriggered event")
    }

    // outbound actions

    test(s"$name doesn't suppress Flush event") {
      pipeline.flush()
      assert(handler.flushFired, "suppressed Flush event")
    }

    test(s"$name doesn't suppress Write event") {
      pipeline.write(new Object)
      assert(handler.writeFired, "suppressed Write event")
    }

    // note: we don't test disconnects because the channel types
    // we care about are connection oriented and disconnect
    // isn't a meaningful operation for them so netty turns them into
    // closes.
    test(s"$name doesn't suppress Close event") {
      pipeline.close()
      assert(handler.closeFired, "suppressed Close event")
    }

    test(s"$name doesn't suppress Deregister event") {
      pipeline.deregister()
      assert(handler.deregisterFired, "suppressed Deregister event")
    }

    test(s"$name doesn't suppress Read event") {
      pipeline.read()
      assert(handler.readFired, "suppressed Read event")
    }

    test(s"$name doesn't suppress Connect event") {
      pipeline.connect(mock[SocketAddress])
      assert(handler.connectFired, "suppressed Connect event")
    }

    test(s"$name doesn't suppress Bind event") {
      pipeline.bind(mock[SocketAddress])
      assert(handler.bindFired, "suppressed Bind event")
    }
  }

  /**
   * a channel duplex handler which records which events have fired
   */
  class TestDuplexHandler extends ChannelDuplexHandler {

    // outbound events
    var flushFired = false
    override def flush(ctx: ChannelHandlerContext): Unit = {
      flushFired = true
      super.flush(ctx)
    }

    var writeFired = false
    override def write(ctx: ChannelHandlerContext, msg: scala.Any, promise: ChannelPromise): Unit = {
      writeFired = true
      super.write(ctx, msg, promise)
    }

    var closeFired = false
    override def close(ctx: ChannelHandlerContext, future: ChannelPromise): Unit = {
      closeFired = true
      super.close(ctx, future)
    }

    var deregisterFired = false
    override def deregister(ctx: ChannelHandlerContext, future: ChannelPromise): Unit = {
      deregisterFired = true
      super.deregister(ctx, future)
    }

    var readFired = false
    override def read(ctx: ChannelHandlerContext): Unit = {
      readFired = true
      super.read(ctx)
    }

    var connectFired = false
    override def connect(ctx: ChannelHandlerContext, remoteAddress: SocketAddress, localAddress: SocketAddress, future: ChannelPromise): Unit = {
      connectFired = true
      super.connect(ctx, remoteAddress, localAddress, future)
    }

    var bindFired = false
    override def bind(ctx: ChannelHandlerContext, localAddress: SocketAddress, future: ChannelPromise): Unit = {
      bindFired = true
      super.bind(ctx, localAddress, future)
    }


    // inbound events
    var exceptionCaughtFired = false
    override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
      exceptionCaughtFired = true
      super.exceptionCaught(ctx, cause)
    }

    var channelActiveFired = false
    override def channelActive(ctx: ChannelHandlerContext): Unit = {
      channelActiveFired = true
      super.channelActive(ctx)
    }

    var channelReadFired = false
    override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
      channelReadFired = true
      super.channelRead(ctx, msg)
    }

    var channelUnregisteredFired = false
    override def channelUnregistered(ctx: ChannelHandlerContext): Unit = {
      channelUnregisteredFired = true
      super.channelUnregistered(ctx)
    }

    var channelInactiveFired = false
    override def channelInactive(ctx: ChannelHandlerContext): Unit = {
      channelInactiveFired = true
      super.channelInactive(ctx)
    }

    var channelWritabilityChangedFired = false
    override def channelWritabilityChanged(ctx: ChannelHandlerContext): Unit = {
      channelWritabilityChangedFired = true
      super.channelWritabilityChanged(ctx)
    }

    var userEventTriggeredFired = false
    override def userEventTriggered(ctx: ChannelHandlerContext, evt: scala.Any): Unit = {
      userEventTriggeredFired = true
      super.userEventTriggered(ctx, evt)
    }

    var channelRegisteredFired = false
    override def channelRegistered(ctx: ChannelHandlerContext): Unit = {
      channelRegisteredFired = true
      super.channelRegistered(ctx)
    }

    var channelReadCompleteFired = false
    override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
      channelReadCompleteFired = true
      super.channelReadComplete(ctx)
    }

    var handlerRemovedFired = false
    override def handlerRemoved(ctx: ChannelHandlerContext): Unit = {
      handlerRemovedFired = true
      super.handlerRemoved(ctx)
    }

    var handlerAddedFired = false
    override def handlerAdded(ctx: ChannelHandlerContext): Unit = {
      handlerAddedFired = true
      super.handlerAdded(ctx)
    }
  }
}
