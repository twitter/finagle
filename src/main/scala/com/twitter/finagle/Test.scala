package com.twitter.finagle

import java.net.{InetSocketAddress, SocketAddress}
import java.util.concurrent.Executors

import org.jboss.netty.channel.socket.nio._
import org.jboss.netty.bootstrap.{ClientBootstrap, ServerBootstrap}
import org.jboss.netty.channel._

import org.apache.thrift.async.AsyncMethodCallback

import com.twitter.silly._
import com.twitter.finagle.thrift._
import com.twitter.finagle.util.{Ok, Error}
import com.twitter.finagle.util.Conversions._

object ThriftServerTest {
  def main(args: Array[String]) {
    val cf = new NioServerSocketChannelFactory(
      Executors.newCachedThreadPool(),
      Executors.newCachedThreadPool())
    val bootstrap = new ServerBootstrap(cf)

    bootstrap.setPipelineFactory(new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("framer", new ThriftFrameCodec)
        pipeline.addLast("codec", new ServerThriftCodec[Silly.Iface](new Object {
          def bleep(s: String, cb: AsyncMethodCallback[String]) = cb.onComplete(s.reverse)
        }))
        pipeline.addLast("handler", new SimpleChannelUpstreamHandler {
          override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
            println("Server received message event: %s".format(e))
            // val result = e.getMessage.asInstanceOf[Silly.bleep_result]
            // println("got reply! %s".format(result.success))
          }
        })
        pipeline
      }
    })

    bootstrap.bind(new InetSocketAddress(6767))
  }
}

object ThriftClientTest {
  def main(args: Array[String]) {
    val cf = new NioClientSocketChannelFactory(
      Executors.newCachedThreadPool(),
      Executors.newCachedThreadPool())

    val bootstrap = new ClientBootstrap(cf)
    bootstrap.setPipelineFactory(new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("framer", new ThriftFrameCodec)
        pipeline.addLast("codec", new ThriftCodec)
        pipeline.addLast("handler", new SimpleChannelUpstreamHandler {
          override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
            val result = e.getMessage.asInstanceOf[Silly.bleep_result]
            println("got reply! %s".format(result.success))
          }
        })
        pipeline
      }
    })

    val f = bootstrap.connect(new InetSocketAddress("localhost", 6767))
    f {
      case Ok(channel) =>
        Channels.write(channel, ThriftCall[Silly.bleep_args, Silly.bleep_result]("bleep", new Silly.bleep_args("heyhey")))
      case Error(_) =>
        println("failed!")
    }

    f.getChannel.getCloseFuture.awaitUninterruptibly()
  }
}
