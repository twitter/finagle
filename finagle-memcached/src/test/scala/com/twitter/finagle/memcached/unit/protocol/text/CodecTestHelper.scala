package com.twitter.finagle.memcached.unit.protocol.text

import java.net.SocketAddress

import com.twitter.util.Promise
import org.jboss.netty.channel._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

class CodecTestHelper extends MockitoSugar {

  var written = new Promise[scala.Any]
  var received = new Promise[scala.Any]

  val channel = mock[Channel]
  when(channel.getRemoteAddress).thenReturn(mock[SocketAddress])
  val msgEvent = mock[MessageEvent]
  when(msgEvent.getFuture).thenReturn(mock[ChannelFuture])
  val context = new ChannelHandlerContext {
    override def sendDownstream(e: ChannelEvent): Unit = e match {
      case msgEvent: DownstreamMessageEvent =>
        written.setValue(msgEvent.getMessage())
    }

    override def sendUpstream(e: ChannelEvent): Unit = e match {
      case msgEvent: UpstreamMessageEvent =>
        received.setValue(msgEvent.getMessage())
    }

    override def getChannel: Channel = channel

    override def getHandler: ChannelHandler = ???

    override def canHandleUpstream: Boolean = ???

    override def getAttachment: AnyRef = ???

    override def getName: String = ???

    override def canHandleDownstream: Boolean = ???

    override def setAttachment(attachment: scala.Any): Unit = ???

    override def getPipeline: ChannelPipeline = ???
  }
}
