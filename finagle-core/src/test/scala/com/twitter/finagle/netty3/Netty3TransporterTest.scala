package com.twitter.finagle.netty3

import com.twitter.conversions.time._
import com.twitter.finagle.Stack
import com.twitter.finagle.client.Transporter.Credentials
import com.twitter.finagle.client.{LatencyCompensation, Transporter}
import com.twitter.finagle.netty3.socks.SocksConnectHandler
import com.twitter.finagle.netty3.transport.ChannelTransport
import com.twitter.finagle.param.Label
import com.twitter.finagle.ssl.Engine
import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver}
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.util.InetSocketAddressUtil
import com.twitter.util.{Await, Duration}
import java.net.InetSocketAddress
import java.nio.channels.UnresolvedAddressException
import javax.net.ssl.{SSLEngine, SSLEngineResult, SSLSession}
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel._
import org.jboss.netty.handler.timeout.IdleStateHandler
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FunSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class Netty3TransporterTest extends FunSpec with MockitoSugar {
  describe("Netty3Transporter") {
    it("creates a Netty3Transporter instance based on Stack params") {
      val inputParams =
        Stack.Params.empty +
          Label("test") +
          Netty3Transporter.TransportFactory.param.default +
          Transporter.ConnectTimeout(1.seconds) +
          LatencyCompensation.Compensation(12.millis) +
          Transporter.TLSHostname(Some("tls.host")) +
          Transporter.HttpProxy(Some(new InetSocketAddress(0)), Some(Credentials("user", "pw"))) +
          Transporter.SocksProxy(Some(new InetSocketAddress(0)), Some("user", "pw")) +
          Transport.BufferSizes(Some(100), Some(200)) +
          Transport.TLSClientEngine.param.default +
          Transport.Liveness(1.seconds, 2.seconds, Some(true)) +
          Transport.Verbose(true)

      val pipelineFactory = Channels.pipelineFactory(Channels.pipeline())
      val transporter = Netty3Transporter.make(pipelineFactory, inputParams)
      assert(transporter.name == inputParams[Label].label)
      assert(transporter.pipelineFactory == pipelineFactory)
      assert(
        transporter.tlsConfig ==
          inputParams[Transport.TLSClientEngine].e.map(
            Netty3TransporterTLSConfig(_, inputParams[Transporter.TLSHostname].hostname)))
      assert(transporter.httpProxy == inputParams[Transporter.HttpProxy].sa)
      assert(transporter.httpProxyCredentials == inputParams[Transporter.HttpProxy].credentials)
      assert(transporter.socksProxy == inputParams[Transporter.SocksProxy].sa)
      assert(transporter.socksUsernameAndPassword == inputParams[Transporter.SocksProxy].credentials)
      assert(transporter.channelReaderTimeout == inputParams[Transport.Liveness].readTimeout)
      assert(transporter.channelWriterTimeout == inputParams[Transport.Liveness].writeTimeout)
      assert(transporter.channelOptions.get("sendBufferSize") == inputParams[Transport.BufferSizes].send)
      assert(transporter.channelOptions.get("receiveBufferSize") == inputParams[Transport.BufferSizes].recv)
      assert(transporter.channelOptions.get("keepAlive") == inputParams[Transport.Liveness].keepAlive)
      assert(transporter.channelOptions.get("connectTimeoutMillis").get ==
        inputParams[Transporter.ConnectTimeout].howlong.inMilliseconds +
        inputParams[LatencyCompensation.Compensation].howlong.inMilliseconds)
      assert(transporter.channelSnooper.nonEmpty == inputParams[Transport.Verbose].enabled)
    }

    it("newPipeline handles unresolved InetSocketAddresses") {
      val pipeline = Channels.pipeline()
      val pipelineFactory = new ChannelPipelineFactory {
        override def getPipeline(): ChannelPipeline = pipeline
      }

      val transporter = new Netty3Transporter[Int, Int](
        "name",
        pipelineFactory,
        socksProxy = Some(InetSocketAddress.createUnresolved("anAddr", 0))
      )

      val unresolved = InetSocketAddress.createUnresolved("supdog", 0)
      val pl = transporter.newPipeline(unresolved, NullStatsReceiver)
      assert(pl == pipeline) // mainly just checking that we don't NPE anymore
    }

    // CSL-2175
    ignore("expose UnresolvedAddressException") {
      val transporter =
        Netty3Transporter[Int, Int]("name", Channels.pipelineFactory(Channels.pipeline()))
      val addr = InetSocketAddressUtil.parseHosts("localhost/127.0.0.1:1234")
      intercept[UnresolvedAddressException] {
          Await.result(transporter(addr.head, new InMemoryStatsReceiver))
        }
    }

    describe("IdleStateHandler") {
      def expectedIdleStateHandler(
        readerTimeout: Duration,
        writerTimeout: Duration,
        isHanlderExist: Boolean
      ) {
        val transporter = new Netty3Transporter[Int, Int](
          "name",
          Channels.pipelineFactory(Channels.pipeline()),
          channelReaderTimeout = readerTimeout,
          channelWriterTimeout = writerTimeout
        )
        val pl = transporter.newPipeline(new InetSocketAddress(0), NullStatsReceiver)
        val idleHandlerFound = pl.toMap.asScala.values.find {
          case _: IdleStateHandler => true
          case _ => false
        }
        assert(idleHandlerFound.nonEmpty == isHanlderExist)
        idleHandlerFound.foreach { h =>
          val ih = h.asInstanceOf[IdleStateHandler]
          assert(ih.getReaderIdleTimeInMillis == readerTimeout.inMilliseconds)
          assert(ih.getWriterIdleTimeInMillis == writerTimeout.inMilliseconds)
          assert(ih.getAllIdleTimeInMillis == 0L)
        }
      }

      it("is added when channelReaderTimeout/channelWriteTimeout are finite") {
        expectedIdleStateHandler(1.seconds, 2.seconds, true)
      }

      it("is not added when neither channelReaderTimeout nor channelWriteTimeout are finite") {
        expectedIdleStateHandler(Duration.Top, Duration.Bottom, false)
      }
    }

    it("should track connections with channelStatsHandler on different connections") {
      val sr = new InMemoryStatsReceiver
      def hasConnections(scope: String, num: Int) {
        assert(sr.gauges(Seq(scope, "connections"))() == num)
      }

      val firstPipeline = Channels.pipeline()
      val secondPipeline = Channels.pipeline()
      val pipelineFactory = new ChannelPipelineFactory {
        override def getPipeline(): ChannelPipeline = firstPipeline
      }
      val transporter = new Netty3Transporter[Int, Int]("name", pipelineFactory)

      val firstHandler = transporter.channelStatsHandler(sr.scope("first"))
      val secondHandler = transporter.channelStatsHandler(sr.scope("second"))

      firstPipeline.addFirst("channelStatsHandler", firstHandler)
      secondPipeline.addFirst("channelStatsHandler", secondHandler)

      hasConnections("first", 0)
      val firstChannel = Netty3Transporter.channelFactory.newChannel(firstPipeline)

      hasConnections("first", 1)
      hasConnections("second", 0)

      val secondChannel = Netty3Transporter.channelFactory.newChannel(secondPipeline)
      Channels.close(firstChannel)


      hasConnections("first", 0)
      hasConnections("second", 1)

      Channels.close(secondChannel)
      hasConnections("second", 0)
    }

    describe("SocksConnectHandler") {
      val loopbackSockAddr = new InetSocketAddress("127.0.0.1", 9999)
      val linkLocalSockAddr = new InetSocketAddress("169.254.0.1", 9999)
      val routableSockAddr = new InetSocketAddress("8.8.8.8", 9999)

      def hasSocksConnectHandler(pipeline: ChannelPipeline) =
        pipeline.toMap.asScala.values.exists {
          case _: SocksConnectHandler => true
          case _ => false
        }

      it ("is not added if no proxy address is given") {
        val pipelineFactory = Channels.pipelineFactory(Channels.pipeline())
        val transporter = new Netty3Transporter[Int, Int]("name", pipelineFactory)
        val pipeline = transporter.newPipeline(loopbackSockAddr, NullStatsReceiver)
        assert(!hasSocksConnectHandler(pipeline))
      }

      it ("is not added if proxy address is given but address isLoopback") {
        val pipelineFactory = Channels.pipelineFactory(Channels.pipeline())
        val transporter = new Netty3Transporter[Int, Int]("name", pipelineFactory, socksProxy = Some(loopbackSockAddr))
        val pipeline = transporter.newPipeline(loopbackSockAddr, NullStatsReceiver)
        assert(!hasSocksConnectHandler(pipeline))
      }

      it ("is not added if proxy address is given but address isLinkLocal") {
        val pipelineFactory = Channels.pipelineFactory(Channels.pipeline())
        val transporter = new Netty3Transporter[Int, Int]("name", pipelineFactory, socksProxy = Some(loopbackSockAddr))
        val pipeline = transporter.newPipeline(linkLocalSockAddr, NullStatsReceiver)
        assert(!hasSocksConnectHandler(pipeline))
      }

      it ("is added if proxy address is given and addr is routable") {
        val pipelineFactory = Channels.pipelineFactory(Channels.pipeline())
        val transporter = new Netty3Transporter[Int, Int]("name", pipelineFactory, socksProxy = Some(loopbackSockAddr))
        val pipeline = transporter.newPipeline(routableSockAddr, NullStatsReceiver)
        assert(hasSocksConnectHandler(pipeline))
      }
    }

    describe("SslHandler") {
      it ("should close the channel if the remote peer closed TLS session") {
        val result = new SSLEngineResult(
          SSLEngineResult.Status.CLOSED, SSLEngineResult.HandshakeStatus.NEED_UNWRAP, 0, 0
        )

        val session = mock[SSLSession]
        val engine = mock[SSLEngine]
        when(engine.getSession) thenReturn session
        when(session.getApplicationBufferSize) thenReturn 1024
        when(engine.unwrap(any[java.nio.ByteBuffer], any[java.nio.ByteBuffer])) thenReturn result
        when(engine.getUseClientMode) thenReturn true
        when(engine.isInboundDone) thenReturn true
        when(engine.getEnableSessionCreation) thenReturn true

        val mockTlsConfig = Netty3TransporterTLSConfig(
          Function.const(new Engine(engine)),
          Some("localhost")
        )

        val pipelineFactory = Channels.pipelineFactory(Channels.pipeline())
        val transporter = new Netty3Transporter[Int, Int](
          "tls-enabled", pipelineFactory, tlsConfig = Some(mockTlsConfig)
        )

        // 21 - alert message, 3 - SSL3 major version,
        // 0 - SSL3 minor version, 0 1 - package length, 0 - close_notify
        val cb = ChannelBuffers.copiedBuffer(Array[Byte](21, 3, 0, 0, 1, 0))
        cb.readerIndex(0)
        cb.writerIndex(6)

        val pipeline = transporter.newPipeline(null, NullStatsReceiver)
        val channel = transporter.newChannel(pipeline)
        new ChannelTransport[Any, Any](channel) // adds itself to the channel's pipeline
        val closeNotify = new UpstreamMessageEvent(channel, cb, null)

        assert(channel.isOpen)

        pipeline.sendUpstream(closeNotify)

        // We wait for I/O thread closing the channel
        assert(channel.getCloseFuture.await(3000)) // timeout 3s
      }
    }
  }
}
