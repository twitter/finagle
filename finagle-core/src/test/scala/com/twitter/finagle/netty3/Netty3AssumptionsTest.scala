package com.twitter.finagle.netty3

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import java.net.{InetSocketAddress, InetAddress}
import java.util.concurrent.Executors
import org.jboss.netty.bootstrap.{ServerBootstrap, ClientBootstrap}
import org.jboss.netty.channel._
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory

import com.twitter.util.CountDownLatch
import com.twitter.conversions.time._

@RunWith(classOf[JUnitRunner])
class Netty3AssumptionsTest extends FunSuite {
  private[this] val executor = Executors.newCachedThreadPool()

  def makeServer() = {
    val bootstrap = new ServerBootstrap(
      new NioServerSocketChannelFactory(executor, executor))
    bootstrap.setPipelineFactory(new ChannelPipelineFactory {
      def getPipeline = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("stfu", new SimpleChannelUpstreamHandler {
          override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
            /* nothing */
          }
        })
        pipeline
      }
    })
    bootstrap.bind(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
  }


  test("Channel.close() should leave the channel in a closed state [immediately]") {

    val ch = makeServer()
    val addr = ch.getLocalAddress
    //doAfter { ch.close().awaitUninterruptibly() }

    val bootstrap = new ClientBootstrap(Netty3Transporter.channelFactory)

    val pipeline = Channels.pipeline
    pipeline.addLast("stfu", new SimpleChannelUpstreamHandler {
      override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
        // nothing here.
      }
    })
    bootstrap.setPipeline(pipeline)

    val latch = new CountDownLatch(1)

    bootstrap.connect(addr).addListener(new ChannelFutureListener {
      override def operationComplete(f: ChannelFuture): Unit =
        if (f.isSuccess) {
          val channel = f.getChannel
          assert(channel.isOpen)
          Channels.close(channel)
          assert(!channel.isOpen)
          latch.countDown()
        } else {
          throw new Exception("connect attempt failed: " + f)
        }
    })

    assert(latch.await(1.second))

    ch.close().awaitUninterruptibly()
  }
}
