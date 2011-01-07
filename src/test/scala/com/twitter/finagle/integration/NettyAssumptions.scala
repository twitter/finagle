package com.twitter.finagle.integration

import org.specs.Specification

import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel._

import com.twitter.util.{Promise, Return}

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.util.Ok

/**
 * Here we test a number of assumptions we are making of Netty. This
 * is all stuff that's verified by examination of the Netty codebase,
 * but the author's semantics aren't necessarily clear. This (might)
 * protect us against Netty upgrades that change our assumptions.
 *
 * And if nothing else, they document the kinds of assumptions we
 * *are* making of Netty :-)
 */
object NettyAssumptionsSpec extends Specification {
  "Channel.close()" should {
    val server = EmbeddedServer()

    // This test, like any involving timing, is of course fraught with
    // races.
    "leave the channel in a closed state [immediately]" in {
      val bootstrap = new ClientBootstrap(
        ClientBuilder.defaultChannelFactory)

      val pipeline = Channels.pipeline
      pipeline.addLast("stfu", new SimpleChannelUpstreamHandler {
        override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
          // nothing here.
        }
      })
      bootstrap.setPipeline(pipeline)

      val latch = new Promise[Unit]

      bootstrap.connect(server.addr) {
        case Ok(channel) =>
          channel.isOpen must beTrue
          Channels.close(channel)
          channel.isOpen must beFalse
          latch() = Return(())
        case _ =>
          throw new Exception("Failed to connect to the expected socket.")
      }

      latch()
    }
  }
}
