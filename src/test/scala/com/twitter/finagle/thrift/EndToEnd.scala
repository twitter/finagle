package com.twitter.finagle.thrift

import scala.util.Random

import java.util.concurrent.Executors
import java.net.{InetAddress, InetSocketAddress, Socket}

import org.specs.Specification

import org.apache.thrift.{TProcessor, TProcessorFactory}
import org.apache.thrift.transport.{
  TSocket, TServerSocket, TFramedTransport}
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.server.TSimpleServer

import org.jboss.netty.bootstrap.{ClientBootstrap, ServerBootstrap}
import org.jboss.netty.channel.{
  Channels, ChannelPipeline, ChannelPipelineFactory,
  SimpleChannelUpstreamHandler, ChannelHandlerContext, MessageEvent}
import org.jboss.netty.channel.local.{
  DefaultLocalServerChannelFactory, DefaultLocalClientChannelFactory,
  LocalAddress}
import org.jboss.netty.channel.socket.nio.{
  NioClientSocketChannelFactory, NioServerSocketChannelFactory}

import com.twitter.util.{Promise, Return, Throw}
import com.twitter.util.TimeConversions._
import com.twitter.silly.Silly

import com.twitter.finagle.util.Conversions._

object PickRandomPort {
  val rng = new Random
  def apply(): Int = {
    val retries = 5
    for (i <- 0 until retries) {
      val port = (math.abs(rng.nextInt) % 65000) + 1024
      val address = new InetSocketAddress(InetAddress.getLocalHost, port)
      val sock = new Socket
      try {
        sock.bind(address)
      } finally {
        sock.close()
      }
      return port
    }
    throw new Exception("Couldn't find an open port")
  }
}

object EndToEndSpec extends Specification {
  class SillyImpl extends Silly.Iface {
    def bleep(bloop: String): String =
      bloop.reverse
  }

  object Codecs {
    abstract class Appender {
      def append(pipeline: ChannelPipeline): Unit = throw new AbstractMethodError
      def processor(): TProcessor = throw new AbstractMethodError
    }

    object ThriftProcessorHandler extends Appender {
      override def processor =
        new Silly.Processor(new SillyImpl)

      override def append(pipeline: ChannelPipeline) {
        val processorFactory = new TProcessorFactory(processor)
        pipeline.addLast("processor", new ThriftProcessorHandler(processorFactory))
      }
    }

    object ThriftCodecAndSeparateHandler extends Appender {
      override def append(pipeline: ChannelPipeline) {
        pipeline.addLast("codec", new ThriftServerCodec)
        pipeline.addLast("handler", new SimpleChannelUpstreamHandler {
          override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
            val msg = e.getMessage.asInstanceOf[ThriftCall[_, _]]
            msg match {
              case bleep: ThriftCall[Silly.bleep_args, Silly.bleep_result] =>
                val args = bleep.args.asInstanceOf[Silly.bleep_args]
                val response = bleep.newResponseInstance
                response.setSuccess(args.request.reverse)
                Channels.write(ctx.getChannel, new ThriftReply[Silly.bleep_result](response, bleep))
              case _ =>
                throw new IllegalArgumentException
            }
          }
        })
      }
    }
  }

  // TODO: test with a traditional thrift stack over local loopback
  // TCP

  "client & server" should {
    def commonClientServerTests(codecUnderTest: Codecs.Appender) = {
      "talk silly to each other (%s)".format(codecUnderTest.getClass.getName) in {
        // ** Set up the server.
        val serverBootstrap = new ServerBootstrap(new DefaultLocalServerChannelFactory())
        serverBootstrap.setPipelineFactory(new ChannelPipelineFactory {
          def getPipeline() = {

            val pipeline = Channels.pipeline()
            pipeline.addLast("framer", new ThriftFrameCodec)
            codecUnderTest.append(pipeline)
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
                Channels.close(ctx.getChannel)
              }
            })

            pipeline
          }
        })

        val addr = new LocalAddress("thrift")
        val serverChannel = serverBootstrap.bind(addr)
        for (ch <- clientBootstrap.connect(addr)) {
          val thriftCall =
            ThriftCall[Silly.bleep_args, Silly.bleep_result](
              "bleep", new Silly.bleep_args("heyhey"))

          Channels.write(ch, thriftCall)
        }

        val result = callResults.within(1.second)
        result.isReturn must beTrue

        result().success must be_==("yehyeh")

        // ** Shutdown
        serverChannel.close().awaitUninterruptibly()
        serverBootstrap.getFactory.releaseExternalResources()
      }
    }

    "ThriftProcessorHandler" in {
      commonClientServerTests(Codecs.ThriftProcessorHandler)
      commonClientServerTests(Codecs.ThriftCodecAndSeparateHandler)
    }
  }

  "client" should {
    "talk silly to an existing server" in {
      // ** Set up a traditional thrift server.
      val serverPort       = PickRandomPort()
      val serverAddr       = new InetSocketAddress("localhost", serverPort)
      val serverSocket     = new TServerSocket(serverPort)
      val transportFactory = new TFramedTransport.Factory
      val protocolFactory  = new TBinaryProtocol.Factory(true, true)

      val thriftServer = new TSimpleServer(
        Codecs.ThriftProcessorHandler.processor, serverSocket,
        transportFactory, protocolFactory)

      val callResults = new Promise[Silly.bleep_result]

      val cf = new NioClientSocketChannelFactory(
        Executors.newCachedThreadPool(),
        Executors.newCachedThreadPool())
      val clientBootstrap = new ClientBootstrap(cf)

      clientBootstrap.setPipelineFactory(new ChannelPipelineFactory {
        def getPipeline() = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("framer", new ThriftFrameCodec)
          pipeline.addLast("codec", new ThriftCodec)
          pipeline.addLast("handler", new SimpleChannelUpstreamHandler {
            override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
              callResults() = Return(e.getMessage.asInstanceOf[Silly.bleep_result])
              Channels.close(ctx.getChannel)
            }
          })

          pipeline
        }
      })

      // ** Do the deed.
      val serverThread =
        new Thread {
          override def run() = thriftServer.serve()
        }
      serverThread.start()

      for (ch <- clientBootstrap.connect(serverAddr)) {
        val thriftCall = new ThriftCall[Silly.bleep_args, Silly.bleep_result](
          "bleep", new Silly.bleep_args("foobar"))

        Channels.write(ch, thriftCall)
      }

      val result = callResults.within(1.second)
      result.isReturn must beTrue

      result().success must be_==("raboof")

      thriftServer.stop()
      serverThread.join()
    }
  }

  "server" should {
    "talk silly with an existing client" in {
      val serverBootstrap = new ServerBootstrap(
        new NioServerSocketChannelFactory(
          Executors.newCachedThreadPool(),
          Executors.newCachedThreadPool()))
      val serverAddress = new InetSocketAddress("localhost", PickRandomPort())

      serverBootstrap.setPipelineFactory(new ChannelPipelineFactory {
        def getPipeline() = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("framer", new ThriftFrameCodec)
          Codecs.ThriftProcessorHandler.append(pipeline)
          pipeline
        }
      })

      val (transport, client) = {
        val socket = new TSocket(serverAddress.getHostName, serverAddress.getPort, 1000/*ms*/)
        val transport = new TFramedTransport(socket)
        val protocol = new TBinaryProtocol(transport)
        (transport, new Silly.Client(protocol))
      }

      // ** Do it!
      serverBootstrap.bind(serverAddress)
      transport.open()
      client.bleep("shiznit") must be_==("tinzihs")

      // ** Teardown
      transport.close()
      serverBootstrap.getFactory.releaseExternalResources()
    }
  }
}

