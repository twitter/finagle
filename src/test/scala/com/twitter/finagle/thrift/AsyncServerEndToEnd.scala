package com.twitter.finagle.thrift

import org.specs.Specification

import org.apache.thrift.TProcessorFactory

import org.jboss.netty.bootstrap.{ClientBootstrap, ServerBootstrap}
import org.jboss.netty.channel.{
  Channels, ChannelPipelineFactory, SimpleChannelUpstreamHandler,
  ChannelHandlerContext, MessageEvent}
import org.jboss.netty.channel.local.{
  DefaultLocalServerChannelFactory, DefaultLocalClientChannelFactory,
  LocalAddress}

import com.twitter.util.{Promise, Return, Throw}
import com.twitter.util.TimeConversions._
import com.twitter.silly.Silly

import com.twitter.finagle.util.Conversions._

object AsyncServerEndToEndSpec extends Specification {
  class SillyImpl extends Silly.Iface {
    def bleep(bloop: String): String =
      bloop.reverse
  }

  // TODO: test with a traditional thrift stack over local loopback
  // TCP

  "talk silly to each other ... asynchronously" in {
    // ** Set up the server.
    val serverBootstrap = new ServerBootstrap(new DefaultLocalServerChannelFactory())
    serverBootstrap.setPipelineFactory(new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("framer", new ThriftFrameCodec)
        val codec = new ThriftServerCodec
        ThriftTypes.add(ThriftCall[Silly.bleep_args, Silly.bleep_result]("bleep", new Silly.bleep_args()))
        pipeline.addLast("codec", codec)
        pipeline.addLast("handler", new SimpleChannelUpstreamHandler {
          override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
            val msg = e.getMessage.asInstanceOf[ThriftCall[_, _]]
            println("message received in handler: %s".format(msg))
            msg match {
              case bleep: ThriftCall[Silly.bleep_args, Silly.bleep_result] =>
                val args = bleep.args.asInstanceOf[Silly.bleep_args]
                println("Bleep: request=%s".format(args.request))
                val response = bleep.newResponseInstance
                response.setSuccess(args.request.reverse)
                println("Sending response %s".format(response))
                Channels.write(ctx.getChannel, new ThriftResponse[Silly.bleep_result](response, bleep))
              case _ =>
                println("The message's type could not be determined")
            }
          }
        })
        pipeline
      }
    })

    val callResults = new Promise[Silly.bleep_result]

    // ** Set up the client.
    val clientBootstrap = new ClientBootstrap(new DefaultLocalClientChannelFactory)
    clientBootstrap.setPipelineFactory(new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("framer", new ThriftFrameCodec)
        pipeline.addLast("codec", new ThriftCodec)
        pipeline.addLast("handler", new SimpleChannelUpstreamHandler {
          override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
            callResults() = Return(e.getMessage.asInstanceOf[Silly.bleep_result])
          }
        })

        pipeline
      }
    })

    val addr = new LocalAddress("thrift-async")
    serverBootstrap.bind(addr)
    for (ch <- clientBootstrap.connect(addr)) {
      val thriftCall =
        ThriftCall[Silly.bleep_args, Silly.bleep_result](
          "bleep", new Silly.bleep_args("heyhey"))

      Channels.write(ch, thriftCall)
    }

    val result = callResults.within(1.second)
    result.isReturn must beTrue

    // TODO: channel teardown (with releaseExternalResources).

    result().success must be_==("yehyeh")
  }
}
