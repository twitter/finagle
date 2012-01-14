package com.twitter.finagle.stats

import com.twitter.finagle.{Codec, CodecFactory, Service}
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.ostrich.stats.Stats
import com.twitter.util.{RandomSocket, Future}

import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}
import org.jboss.netty.handler.codec.frame.{Delimiters, DelimiterBasedFrameDecoder}
import org.jboss.netty.handler.codec.string.{StringEncoder, StringDecoder}
import org.jboss.netty.util.CharsetUtil

import org.specs.Specification
import org.specs.mock.Mockito


object FinagleStatsSpec extends Specification with Mockito {

  val dummyService = new Service[String, String] {
    def apply(request: String) = Future.value("You said: " + request)
  }

  class StringCodec extends CodecFactory[String, String] {
    def server = Function.const {
      new Codec[String, String] {
        def pipelineFactory = new ChannelPipelineFactory {
          def getPipeline = {
            val pipeline = Channels.pipeline()
            pipeline.addLast("line", new DelimiterBasedFrameDecoder(100, Delimiters.lineDelimiter: _*))
            pipeline.addLast("stringDecoder", new StringDecoder(CharsetUtil.UTF_8))
            pipeline.addLast("stringEncoder", new StringEncoder(CharsetUtil.UTF_8))
            pipeline
          }
        }
      }
    }

    def client = Function.const {
      new Codec[String, String] {
        def pipelineFactory = new ChannelPipelineFactory {
          def getPipeline = {
            val pipeline = Channels.pipeline()
            pipeline.addLast("stringEncode", new StringEncoder(CharsetUtil.UTF_8))
            pipeline.addLast("stringDecode", new StringDecoder(CharsetUtil.UTF_8))
            pipeline
          }
        }
      }
    }
  }

  val statsReceiver = new OstrichStatsReceiver
  val codec = new StringCodec
  val addr = RandomSocket()
  val server = ServerBuilder()
    .name("server")
    .bindTo(addr)
    .codec(codec)
    .reportTo(statsReceiver)
    .maxConcurrentRequests(5)
    .build(dummyService)

  val service = ClientBuilder()
    .name("client")
    .reportTo(statsReceiver)
    .hosts(Seq(addr))
    .codec(codec)
    .hostConnectionLimit(10)
    .build()

  "Finagle stats system" should {

    "correctely count connection" in {
      Stats.getGauge("server/connections") must beSome(0.0)
      Stats.getGauge("client/connections") must beNone

      service("Hello\n").get()
      Stats.getGauge("server/connections") must beSome(1.0)
      Stats.getGauge("client/connections") must beSome(1.0)
    }

    "show symetric stats on client and server" in {
      def equalsGauge(name: String) =
        Stats.getCounter("server/" + name)() mustEqual Stats.getCounter("client/" + name)()

      equalsGauge("requests")
      equalsGauge("connects")
      equalsGauge("success")
    }

  }
}