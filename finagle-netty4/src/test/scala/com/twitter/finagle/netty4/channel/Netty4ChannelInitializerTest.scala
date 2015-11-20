package com.twitter.finagle.netty4.channel

import com.twitter.finagle.Stack.Params
import com.twitter.finagle.param.Stats
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.util.Duration
import io.netty.buffer.Unpooled
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Netty4ChannelInitializerTest
  extends FunSuite
  with Eventually
  with IntegrationPatience {

  val writeDiscardHandler = new ChannelOutboundHandlerAdapter {
    override def write(ctx: ChannelHandlerContext, msg: scala.Any, promise: ChannelPromise): Unit = ()
  }

  val nop = new ChannelHandler {
    def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = ()
    def handlerRemoved(ctx: ChannelHandlerContext): Unit = ()
    def handlerAdded(ctx: ChannelHandlerContext): Unit = ()
  }

  trait Ctx {
    val sr = new InMemoryStatsReceiver
    val srv: SocketChannel = new NioSocketChannel()
    val loop = new NioEventLoopGroup()
    loop.register(srv)
    val baseParams = Params.empty + Stats(sr)
  }


  test("Netty4ChannelInitializer channel writes can time out") {
    new Ctx {
      val params = baseParams + Transport.Liveness(
        readTimeout = Duration.Top,
        writeTimeout = Duration.fromMilliseconds(1),
        keepAlive = None)

      val init = new Netty4ChannelInitializer(_ => (), params, () => nop)
      init.initChannel(srv)

      srv.pipeline.addBefore("writeCompletionTimeout", "writeDiscardHandler", writeDiscardHandler)

      // WriteCompletionTimeoutHandler throws a WriteTimeOutException after the message is lost.
      srv.writeAndFlush("hi")

      // ChannelExceptionHandler records it.
      eventually {
        assert(sr.counters(Seq("write_timeout")) == 1)
      }
    }
  }

  test("Netty4ChannelInitializer channel reads can time out") {
    new Ctx {
      val params = baseParams + Transport.Liveness(
        readTimeout = Duration.fromMilliseconds(1),
        writeTimeout = Duration.Top,
        keepAlive = None)

      val init = new Netty4ChannelInitializer(_ => (), params, () => nop)
      init.initChannel(srv)

      srv.pipeline.fireChannelActive()
      srv.pipeline.fireChannelRead(Unpooled.EMPTY_BUFFER)
      Thread.sleep(10) // We need at least one ms to elapse between read and readComplete.
      // Netty's read timeout handler uses System.nanoTime
      // to mark time so we're stuck sleeping.
      srv.pipeline.fireChannelReadComplete()

      eventually {
        assert(sr.counters(Seq("read_timeout")) == 1)
      }
    }
  }
}
