package com.twitter.finagle.netty3

import java.net.{UnknownHostException, InetAddress, InetSocketAddress, SocketAddress}

import com.twitter.finagle.InconsistentStateException
import org.jboss.netty.channel._
import org.junit.runner.RunWith
import org.mockito.ArgumentCaptor
import org.mockito.Matchers.{eq => eqTo, _}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class SocketAddressResolveHandlerTest extends FunSuite with MockitoSugar {
  class SocketAddressResolveHandlerHelper {
    val ctx = mock[ChannelHandlerContext]
    val channel = mock[Channel]
    when(ctx.getChannel) thenReturn channel
    val pipeline = mock[ChannelPipeline]
    doAnswer(new Answer[ChannelFuture] {
      override def answer(invocation: InvocationOnMock) = {
        invocation.getArguments.headOption.foreach {
          case r: Runnable => r.run()
        }
        mock[ChannelFuture]
      }
    }).when(pipeline).execute(any[Runnable])
    when(ctx.getPipeline) thenReturn pipeline
    when(channel.getPipeline) thenReturn pipeline
    val closeFuture = Channels.future(channel)
    when(channel.getCloseFuture) thenReturn closeFuture
    val remoteAddress = new InetSocketAddress(InetAddress.getLoopbackAddress, 80)
    when(channel.getRemoteAddress) thenReturn remoteAddress
    val channelFuture = Channels.future(channel, true)
    val resolver = mock[SocketAddressResolver]

    def handleSocketAddress(proxyAddress: SocketAddress) {
      val connectRequested = new DownstreamChannelStateEvent(
        channel, channelFuture, ChannelState.CONNECTED, proxyAddress)
      val handler = new SocketAddressResolveHandler(resolver, remoteAddress)
      handler.handleDownstream(ctx, connectRequested)
    }

    def assertClosed() {
      val ec = ArgumentCaptor.forClass(classOf[DownstreamChannelStateEvent])
      verify(pipeline).sendDownstream(ec.capture)
      val e = ec.getValue
      assert(e.getChannel == channel)
      assert(e.getFuture == closeFuture)
      assert(e.getState == ChannelState.OPEN)
      assert(e.getValue == java.lang.Boolean.FALSE)
    }
  }

  test("SocketAddressResolveHandler should resolve unresolved socket address") {
    val helper = new SocketAddressResolveHandlerHelper
    import helper._

    val resolvedInetAddress = InetAddress.getLoopbackAddress
    val unresolvedAddress = InetSocketAddress.createUnresolved("meow.meow", 2222)
    when(resolver.apply(eqTo(unresolvedAddress.getHostName)))
        .thenReturn(Right(resolvedInetAddress))

    handleSocketAddress(unresolvedAddress)

    val ec = ArgumentCaptor.forClass(classOf[DownstreamChannelStateEvent])
    verify(ctx).sendDownstream(ec.capture)
    val e = ec.getValue

    assert(e.getChannel == channel)
    assert(e.getFuture == channelFuture)
    assert(e.getState == ChannelState.CONNECTED)
    assert(e.getValue.isInstanceOf[InetSocketAddress])
    e.getValue match {
      case address: InetSocketAddress => assert(address.getAddress == resolvedInetAddress)
      case _ =>
    }
  }

  test("SocketAddressResolveHandler should close unresolved socket address") {
    val helper = new SocketAddressResolveHandlerHelper
    import helper._

    val unresolvedAddress = InetSocketAddress.createUnresolved("meow.meow", 2222)
    when(resolver.apply(eqTo(unresolvedAddress.getHostName)))
      .thenReturn(Left(new UnknownHostException(unresolvedAddress.getHostName)))

    handleSocketAddress(unresolvedAddress)
    assertClosed()
  }

  test("SocketAddressResolveHandler should close with resolved socket address") {
    val helper = new SocketAddressResolveHandlerHelper
    import helper._

    handleSocketAddress(new InetSocketAddress(InetAddress.getLoopbackAddress, 80))
    assert(channelFuture.getCause.isInstanceOf[InconsistentStateException])
    assertClosed()
  }
}
