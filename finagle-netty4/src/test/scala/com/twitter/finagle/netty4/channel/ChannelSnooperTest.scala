package com.twitter.finagle.netty4.channel

import io.netty.buffer.{ByteBuf, ByteBufUtil}
import io.netty.buffer.Unpooled.wrappedBuffer
import io.netty.channel._
import java.net.InetSocketAddress
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class ChannelSnooperTest extends AnyFunSuite with MockitoSugar {

  val msg = "buffer content"
  val msgBuffer = wrappedBuffer(msg.getBytes("UTF-8"))

  test("ByteBufSnooper decodes and prints inbound and outbound messages") {
    var messageCount = 0
    val bbs = new ByteBufSnooper("bbs") {
      override def dump(printer: (Channel, String) => Unit, ch: Channel, buf: ByteBuf): Unit = {
        messageCount += 1
        assert(buf == msgBuffer)
        super.dump(
          { (_: Channel, m: String) => assert(ByteBufUtil.hexDump(msgBuffer) == m) },
          ch,
          buf)
      }
    }

    bbs.channelRead(mock[ChannelHandlerContext], msgBuffer)
    bbs.write(mock[ChannelHandlerContext], msgBuffer, mock[ChannelPromise])
    assert(messageCount == 2)
  }

  trait InstrumentedSnooperCtx {
    var eventCount = 0
    var inboundCount = 0
    var outboundCount = 0
    var exnCount = 0

    val ctx = mock[ChannelHandlerContext]
    val cid = mock[ChannelId]
    when(cid.asShortText).thenReturn("1")
    val ch = mock[Channel]
    when(ctx.channel()).thenReturn(ch)
    when(ch.remoteAddress()).thenReturn(new InetSocketAddress(80))
    when(ch.id).thenReturn(cid)

    val scs = new SimpleChannelSnooper("scs") {
      override def printInbound(ch: Channel, message: String): Unit =
        inboundCount += 1

      override def printOutbound(ch: Channel, message: String): Unit =
        outboundCount += 1

      override def printer(message: String, exc: Throwable): Unit =
        exnCount += 1

      override def printEvent(ch: Channel, eventName: String): Unit =
        eventCount += 1
    }
  }

  test("SimpleChannelSnooper prints incoming and outgoing messages") {
    new InstrumentedSnooperCtx {
      scs.channelRead(ctx, msgBuffer)
      scs.write(ctx, msgBuffer, mock[ChannelPromise])
      assert(inboundCount == 1)
      assert(outboundCount == 1)
    }
  }

  test("SimpleChannelSnooper snoops exceptionCaught") {
    new InstrumentedSnooperCtx {
      assert(exnCount == 0)
      scs.exceptionCaught(ctx, new Exception)
      assert(exnCount == 1)
    }
  }

  // outbound events
  test("SimpleChannelSnooper snoops write") {
    new InstrumentedSnooperCtx {
      assert(outboundCount == 0)
      scs.write(ctx, msg, mock[ChannelPromise])
      assert(outboundCount == 1)
    }
  }

  test("SimpleChannelSnooper snoops disconnect") {
    new InstrumentedSnooperCtx {
      assert(eventCount == 0)
      scs.disconnect(ctx, mock[ChannelPromise])
      assert(eventCount == 1)
    }
  }

  test("SimpleChannelSnooper snoops flush") {
    new InstrumentedSnooperCtx {
      assert(eventCount == 0)
      scs.flush(ctx)
      assert(eventCount == 1)
    }
  }

  test("SimpleChannelSnooper snoops close") {
    new InstrumentedSnooperCtx {
      assert(eventCount == 0)
      scs.close(ctx, mock[ChannelPromise])
      assert(eventCount == 1)
    }
  }

  test("SimpleChannelSnooper snoops deregister") {
    new InstrumentedSnooperCtx {
      assert(eventCount == 0)
      scs.deregister(ctx, mock[ChannelPromise])
      assert(eventCount == 1)
    }
  }

  test("SimpleChannelSnooper snoops read") {
    new InstrumentedSnooperCtx {
      assert(eventCount == 0)
      scs.read(ctx)
      assert(eventCount == 1)
    }
  }

  test("SimpleChannelSnooper snoops connect") {
    new InstrumentedSnooperCtx {
      assert(eventCount == 0)
      scs.connect(ctx, new InetSocketAddress(0), new InetSocketAddress(0), mock[ChannelPromise])
      assert(eventCount == 1)
    }
  }

  test("SimpleChannelSnooper snoops bind") {
    new InstrumentedSnooperCtx {
      assert(eventCount == 0)
      scs.bind(ctx, new InetSocketAddress(0), mock[ChannelPromise])
      assert(eventCount == 1)
    }
  }

  // inbound events
  test("SimpleChannelSnooper snoops channelActive") {
    new InstrumentedSnooperCtx {
      assert(eventCount == 0)
      scs.channelActive(ctx)
      assert(eventCount == 1)
    }
  }

  test("SimpleChannelSnooper snoops channelUnregistered") {
    new InstrumentedSnooperCtx {
      assert(eventCount == 0)
      scs.channelUnregistered(ctx)
      assert(eventCount == 1)
    }
  }

  test("SimpleChannelSnooper snoops channelInactive") {
    new InstrumentedSnooperCtx {
      assert(eventCount == 0)
      scs.channelInactive(ctx)
      assert(eventCount == 1)
    }
  }

  test("SimpleChannelSnooper snoops channelWritabilityChanged") {
    new InstrumentedSnooperCtx {
      assert(eventCount == 0)
      scs.channelWritabilityChanged(ctx)
      assert(eventCount == 1)
    }
  }

  test("SimpleChannelSnooper snoops userEventTriggered") {
    new InstrumentedSnooperCtx {
      assert(eventCount == 0)
      scs.userEventTriggered(ctx, new Object)
      assert(eventCount == 1)
    }
  }

  test("SimpleChannelSnooper snoops channelRegistered") {
    new InstrumentedSnooperCtx {
      assert(eventCount == 0)
      scs.channelRegistered(ctx)
      assert(eventCount == 1)
    }
  }

  test("SimpleChannelSnooper snoops channelReadComplete") {
    new InstrumentedSnooperCtx {
      assert(eventCount == 0)
      scs.channelReadComplete(ctx)
      assert(eventCount == 1)
    }
  }

  test("SimpleChannelSnooper snoops channelRead") {
    new InstrumentedSnooperCtx {
      assert(inboundCount == 0)
      scs.channelRead(ctx, msg)
      assert(inboundCount == 1)
    }
  }
}
