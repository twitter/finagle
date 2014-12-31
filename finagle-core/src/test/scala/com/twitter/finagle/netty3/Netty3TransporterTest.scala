package com.twitter.finagle.netty3

import com.twitter.finagle.socks.SocksConnectHandler
import com.twitter.finagle.stats.{NullStatsReceiver, InMemoryStatsReceiver}
import com.twitter.util.Duration
import java.net.InetSocketAddress
import org.jboss.netty.channel.{Channels, ChannelPipeline, ChannelPipelineFactory}
import org.jboss.netty.handler.timeout.IdleStateHandler
import org.junit.runner.RunWith
import org.scalatest.FunSpec
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class Netty3TransporterTest extends FunSpec {
  describe("Netty3Transporter") {

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
      assert(pl === pipeline) // mainly just checking that we don't NPE anymore
    }

    it("newPipeline doesn't create IdleStateHandler unnecessarily") {
      val pipeline = Channels.pipeline()
      val pipelineFactory = new ChannelPipelineFactory {
        override def getPipeline(): ChannelPipeline = pipeline
      }

      val transporter = new Netty3Transporter[Int, Int](
        "name",
        pipelineFactory,
        channelReaderTimeout = Duration.Bottom,
        channelWriterTimeout = Duration.Bottom
      )
      val pl = transporter.newPipeline(new InetSocketAddress(0), NullStatsReceiver)
      val idleHandlerFound = pl.toMap.asScala.values.exists {
        case _: IdleStateHandler => true
        case _ => false
      }
      assert(!idleHandlerFound)
    }

    it("should track connections with channelStatsHandler on different connections") {
      val sr = new InMemoryStatsReceiver
      def hasConnections(scope: String, num: Int) {
        assert(sr.gauges(Seq(scope, "connections"))() === num)
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
  }
}
