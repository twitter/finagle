package com.twitter.finagle.thrift

import com.twitter.finagle.netty3.Conversions._
import com.twitter.silly.Silly
import com.twitter.util.TimeConversions._
import com.twitter.util.{Await, Promise, Return, Try}
import org.apache.thrift.protocol.TBinaryProtocol
import org.jboss.netty.bootstrap.{ClientBootstrap, ServerBootstrap}
import org.jboss.netty.channel._
import org.jboss.netty.channel.local._
import org.specs.SpecificationWithJUnit

class AsyncServerEndToEndSpec extends SpecificationWithJUnit {
  val protocolFactory = new TBinaryProtocol.Factory()

  "async Thrift server" should {
    "work" in {
      // ** Set up the server.

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

      // ** Set up the client.
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
      for (ch <- clientBootstrap.connect(addr)) {
        val thriftCall =
          new ThriftCall[Silly.bleep_args, Silly.bleep_result](
            "bleep",
            new Silly.bleep_args("heyhey"),
            classOf[Silly.bleep_result])
        Channels.write(ch, thriftCall)
      }

      val result = Try(Await.result(callResults, 1.second))
      result.isReturn must beTrue

      result().response.success must be_==("yehyeh")

      serverChannel.close().awaitUninterruptibly()
      serverBootstrap.getFactory.releaseExternalResources()
    }
  }
}
