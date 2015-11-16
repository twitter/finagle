package com.twitter.finagle.thrift

import com.twitter.silly.Silly
import com.twitter.util.TimeConversions._
import com.twitter.util.{Await, Promise, Return, Try}
import org.jboss.netty.bootstrap.{ClientBootstrap, ServerBootstrap}
import org.jboss.netty.channel._
import org.jboss.netty.channel.local._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AsyncServerEndToEndTest extends FunSuite {
  val protocolFactory = Protocols.binaryFactory()

  test("async Thrift server should work"){
    // Set up the server.

    ThriftTypes.add(new ThriftCallFactory[Silly.bleep_args, Silly.bleep_result](
      "bleep", classOf[Silly.bleep_args], classOf[Silly.bleep_result]))

    val serverBootstrap = new ServerBootstrap(new DefaultLocalServerChannelFactory())
    serverBootstrap.setPipelineFactory(new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("framer", new ThriftFrameCodec)
        pipeline.addLast("decode", new ThriftServerDecoder(protocolFactory))
        pipeline.addLast("encode", new ThriftServerEncoder(protocolFactory))
        pipeline.addLast("handler", new SimpleChannelUpstreamHandler {
          override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
            e.getMessage match {
              case bleep: ThriftCall[Silly.bleep_args, Silly.bleep_result]
              if bleep.method.equals("bleep") =>
                val response = bleep.newReply
                response.setSuccess(bleep.arguments.request.reverse)
                Channels.write(ctx.getChannel, bleep.reply(response))
              case _ =>
                throw new IllegalArgumentException
            }
          }
        })
        pipeline
      }
    })

    val callResults = new Promise[ThriftReply[Silly.bleep_result]]

    // Set up the client.
    val clientBootstrap = new ClientBootstrap(new DefaultLocalClientChannelFactory)
    clientBootstrap.setPipelineFactory(new ChannelPipelineFactory {
      def getPipeline() = {
        val pipeline = Channels.pipeline()
        pipeline.addLast("framer", new ThriftFrameCodec)
        pipeline.addLast("decode", new ThriftClientDecoder(protocolFactory))
        pipeline.addLast("encode", new ThriftClientEncoder(protocolFactory))
        pipeline.addLast("handler", new SimpleChannelUpstreamHandler {
          override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
            callResults() = Return(e.getMessage.asInstanceOf[ThriftReply[Silly.bleep_result]])
          }
        })

        pipeline
      }
    })

    val addr = new LocalAddress("thrift-async")
    val serverChannel = serverBootstrap.bind(addr)
    clientBootstrap.connect(addr).addListener(new ChannelFutureListener {
      override def operationComplete(f: ChannelFuture): Unit =
        if (f.isSuccess) {
          val ch = f.getChannel
          val thriftCall =
            new ThriftCall[Silly.bleep_args, Silly.bleep_result](
              "bleep",
              new Silly.bleep_args("heyhey"),
              classOf[Silly.bleep_result])
          Channels.write(ch, thriftCall)
        }
    })

    val result = Try(Await.result(callResults, 1.second))
    assert(result.isReturn == true)

    assert(result().response.success  == "yehyeh")

    serverChannel.close().awaitUninterruptibly()
    serverBootstrap.getFactory.releaseExternalResources()
  }
}
