package com.twitter.finagle.netty3

import com.twitter.finagle.stats.InMemoryStatsReceiver
import org.jboss.netty.channel.{Channels, ChannelPipeline, ChannelPipelineFactory}
import org.junit.runner.RunWith
import org.scalatest.FunSpec
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Netty3TransporterTest extends FunSpec {
  describe("Netty3Transporter") {

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
  }
}
