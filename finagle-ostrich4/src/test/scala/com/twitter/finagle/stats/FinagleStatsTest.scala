package com.twitter.finagle.stats

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import com.twitter.finagle.{Codec, CodecFactory, Service}
import com.twitter.util.{Await, Future}
import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}
import org.jboss.netty.handler.codec.frame.{Delimiters, DelimiterBasedFrameDecoder}
import org.jboss.netty.handler.codec.string.{StringEncoder, StringDecoder}
import com.twitter.io.Charsets
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import java.net.{InetAddress, InetSocketAddress}
import com.twitter.ostrich.stats.Stats

@RunWith(classOf[JUnitRunner])
class FinagleStatsTest extends FunSuite with MockitoSugar {
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
            pipeline.addLast("stringDecoder", new StringDecoder(Charsets.Utf8))
            pipeline.addLast("stringEncoder", new StringEncoder(Charsets.Utf8))
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
            pipeline.addLast("stringEncode", new StringEncoder(Charsets.Utf8))
            pipeline.addLast("stringDecode", new StringDecoder(Charsets.Utf8))
            pipeline
          }
        }
      }
    }
  }

  val statsReceiver = new OstrichStatsReceiver
  val codec = new StringCodec
  val server = ServerBuilder()
    .name("server")
    .bindTo(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
    .codec(codec)
    .reportTo(statsReceiver)
    .maxConcurrentRequests(5)
    .build(dummyService)

  val service = ClientBuilder()
    .name("client")
    .reportTo(statsReceiver)
    .hosts(server.boundAddress)
    .codec(codec)
    .hostConnectionLimit(10)
    .build()

  test("system should correctly count connections") {
    /*TODO: is this ok? We are not registering connections gauge until connection
    is needed.
    Stats.getGauge("server/connections") must beSome(0.0)
    Stats.getGauge("client/connections") must beSome(0.0)*/

    Await.result(service("Hello\n"))
    assert(Stats.getGauge("server/connections") == Some(1.0))
    assert(Stats.getGauge("client/connections") == Some(1.0))
  }

  test("system should show symmetric stats on client and server") {
    def equalsGauge(name: String) =
      assert(Stats.getCounter("server/" + name)() == Stats.getCounter("client/" + name)())

    equalsGauge("requests")
    equalsGauge("connects")
    equalsGauge("success")
  }
}
