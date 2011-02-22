package com.twitter.finagle.thrift

import org.specs.Specification

import org.apache.thrift.{TProcessorFactory, TApplicationException}
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.server.TSimpleServer
import org.apache.thrift.transport.{TSocket, TServerSocket, TFramedTransport}
import java.util.concurrent.Executors

import org.jboss.netty.bootstrap.{ClientBootstrap, ServerBootstrap}
import org.jboss.netty.channel._
import org.jboss.netty.channel.local._
import org.jboss.netty.channel.socket.nio._

import com.twitter.test.{B, SomeStruct, AnException, F}
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.util.Conversions._
import com.twitter.silly.Silly
import com.twitter.util.{Future, RandomSocket, Return, Promise}
import com.twitter.util.TimeConversions._

object EndToEndSpec extends Specification {
  "Thrift server" should {
    val processor =  new B.ServiceIface {
      def add(a: Int, b: Int) = Future.exception(new AnException)
      def add_one(a: Int, b: Int) = Future.void
      def multiply(a: Int, b: Int) = Future { a * b }
      def complex_return(someString: String) = Future {
        Trace.record("hey it's me!")
        new SomeStruct(123, Trace().traceID.parentSpan.get.toString)
      }
      def someway() = Future.void
    }

    val serverAddr = RandomSocket()
    val server = ServerBuilder()
      .codec(ThriftServerFramedCodec())
      .bindTo(serverAddr)
      .build(new B.Service(processor, new TBinaryProtocol.Factory()))

    doAfter { server.close(20.milliseconds) }

    val service = ClientBuilder()
      .hosts(Seq(serverAddr))
      .codec(ThriftClientFramedCodec())
      .build()

    val client = new B.ServiceToClient(service, new TBinaryProtocol.Factory())

    "work" in {
      val future = client.multiply(10, 30)
      future() must be_==(300)

      import com.twitter.finagle.tracing.BufferingTranscript
      Trace().transcript = new BufferingTranscript(Trace().traceID)

      client.complex_return("a string")().arg_two must be_==(
        "%s".format(Trace().traceID.span.toString))

      Trace().transcript must haveSize(1)
      Trace().transcript.head.message must be_==("hey it's me!")

      client.add(1, 2)() must throwA[AnException]
      client.add_one(1, 2)()  // don't block!

      client.someway()() must beNull  // don't block!
    }

    "handle wrong interface" in {
      val client = new F.ServiceToClient(service, new TBinaryProtocol.Factory())
     
      client.another_method(123)() must throwA(
        new TApplicationException("Invalid method name: 'another_method'"))
    }
  }

  "netty client & server (old style)" should {
    "talk silly to each other" in {
      ThriftTypes.add(new ThriftCallFactory[Silly.bleep_args, Silly.bleep_result](
        "bleep", classOf[Silly.bleep_args], classOf[Silly.bleep_result]))

      // ** Set up the server.
      val serverBootstrap = new ServerBootstrap(new DefaultLocalServerChannelFactory())
      serverBootstrap.setPipelineFactory(new ChannelPipelineFactory {
        def getPipeline() = {
          val processor = new Silly.Processor(new Silly.Iface {
            def bleep(request: String): String =
              request.reverse
          })
          val processorFactory = new TProcessorFactory(processor)

          val pipeline = Channels.pipeline()
          pipeline.addLast("framer", new ThriftFrameCodec)
          pipeline.addLast("processor", new ThriftProcessorHandler(processorFactory))
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
          pipeline.addLast("encoder", new ThriftClientEncoder)
          pipeline.addLast("decoder", new ThriftClientDecoder)
          pipeline.addLast("handler", new SimpleChannelUpstreamHandler {
            override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
              callResults() = Return(e.getMessage.asInstanceOf[ThriftReply[Silly.bleep_result]])
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
          new ThriftCall[Silly.bleep_args, Silly.bleep_result](
            "bleep", new Silly.bleep_args("heyhey"), classOf[Silly.bleep_result])

        Channels.write(ch, thriftCall)
      }

      val result = callResults.get(1.second)
      result.isReturn must beTrue

      result().response.success must be_==("yehyeh")

      // ** Shutdown
      serverChannel.close().awaitUninterruptibly()
      serverBootstrap.getFactory.releaseExternalResources()
    }
  }

  "netty client" should {
    "talk silly to an existing server" in {
      ThriftTypes.add(new ThriftCallFactory[Silly.bleep_args, Silly.bleep_result](
        "bleep", classOf[Silly.bleep_args], classOf[Silly.bleep_result]))

      // ** Set up a traditional thrift server.
      val serverAddr       = RandomSocket()
      val serverSocket     = new TServerSocket(serverAddr.getPort)
      val transportFactory = new TFramedTransport.Factory
      val protocolFactory  = new TBinaryProtocol.Factory(true, true)

      val processor = new Silly.Processor(new Silly.Iface {
        def bleep(bloop: String): String =
          bloop.reverse
      })
      val thriftServer = new TSimpleServer(
        processor, serverSocket,
        transportFactory, protocolFactory)

      val callResults = new Promise[ThriftReply[Silly.bleep_result]]

      val cf = new NioClientSocketChannelFactory(
        Executors.newCachedThreadPool(),
        Executors.newCachedThreadPool())
      val clientBootstrap = new ClientBootstrap(cf)

      clientBootstrap.setPipelineFactory(new ChannelPipelineFactory {
        def getPipeline() = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("framer", new ThriftFrameCodec)
          pipeline.addLast("encoder", new ThriftClientEncoder)
          pipeline.addLast("decoder", new ThriftClientDecoder)
          pipeline.addLast("handler", new SimpleChannelUpstreamHandler {
            override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
              callResults() = Return(e.getMessage.asInstanceOf[ThriftReply[Silly.bleep_result]])
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
          "bleep", new Silly.bleep_args("foobar"), classOf[Silly.bleep_result])

        Channels.write(ch, thriftCall)
      }

      val result = callResults.get(1.second)
      result.isReturn must beTrue

      result().response.success must be_==("raboof")

      thriftServer.stop()
      serverThread.join()
    }
  }

  "netty server" should {
    "talk silly with an existing client" in {
      val serverBootstrap = new ServerBootstrap(
        new NioServerSocketChannelFactory(
          Executors.newCachedThreadPool(),
          Executors.newCachedThreadPool()))
      val serverAddress = RandomSocket()

      serverBootstrap.setPipelineFactory(new ChannelPipelineFactory {
        def getPipeline() = {
          val processor = new Silly.Processor(new Silly.Iface {
            def bleep(bloop: String): String =
              bloop.reverse
          })
          val processorFactory = new TProcessorFactory(processor)
          val pipeline = Channels.pipeline()
          pipeline.addLast("framer", new ThriftFrameCodec)
          pipeline.addLast("processor", new ThriftProcessorHandler(processorFactory))
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
