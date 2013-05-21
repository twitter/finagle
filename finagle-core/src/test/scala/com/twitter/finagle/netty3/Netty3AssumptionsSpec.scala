package com.twitter.finagle.netty3

import java.util.concurrent.Executors
import org.jboss.netty.bootstrap.{ServerBootstrap, ClientBootstrap}
import org.jboss.netty.channel._
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import org.specs.SpecificationWithJUnit

import Conversions._

import com.twitter.util.{CountDownLatch, RandomSocket}
import com.twitter.conversions.time._

/**
 * Here we test a number of assumptions we are making of Netty. This
 * is all stuff that's verified by examination of the Netty codebase,
 * but the author's semantics aren't necessarily clear. This (might)
 * protect us against Netty upgrades that change our assumptions.
 *
 * And if nothing else, they document the kinds of assumptions we
 * *are* making of Netty :-)
 */
class Netty3AssumptionsSpec extends SpecificationWithJUnit {
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
    bootstrap.bind(RandomSocket())
  }

  "Channel.close()" should {
    val ch = makeServer()
    val addr = ch.getLocalAddress()
    doAfter { ch.close().awaitUninterruptibly() }

    // This test, like any involving timing, is of course fraught with
    // races.
    "leave the channel in a closed state [immediately]" in {
      val bootstrap = new ClientBootstrap(Netty3Transporter.channelFactory)

      val pipeline = Channels.pipeline
      pipeline.addLast("stfu", new SimpleChannelUpstreamHandler {
        override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
          // nothing here.
        }
      })
      bootstrap.setPipeline(pipeline)

      val latch = new CountDownLatch(1)

      bootstrap.connect(addr) {
        case Ok(channel) =>
          channel.isOpen must beTrue
          Channels.close(channel)
          channel.isOpen must beFalse
          latch.countDown()
        case wtf =>
          throw new Exception("connect attempt failed: "+wtf)
      }

      latch.await(1.second) must beTrue
    }
  }
}
