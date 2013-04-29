package com.twitter.finagle.builder

import com.twitter.conversions.time._
import com.twitter.finagle.{Service, TooManyConcurrentRequestsException}
import com.twitter.util.Future
import org.specs.SpecificationWithJUnit
import java.net.InetSocketAddress
import com.twitter.finagle.ChannelClosedException
import com.twitter.finagle.channel.OpenConnectionsThresholds
import com.twitter.util.{Future, CountDownLatch, Promise}

import com.twitter.finagle.{Codec, CodecFactory}
import org.jboss.netty.handler.codec.string.{StringEncoder, StringDecoder}
import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}
import org.jboss.netty.handler.codec.frame.{Delimiters, DelimiterBasedFrameDecoder}
import org.jboss.netty.util.CharsetUtil

/**
 * This Codec is a newline (\n) delimited line-based protocol. Here we re-use existing 
 * encoders/decoders as provided by Netty. This codec allows us to make requests which
 * are incomplete due to missing newline (\n)
 */
object StringCodec extends StringCodec

class StringCodec extends CodecFactory[String, String] {
  def server = Function.const {
    new Codec[String, String] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("line",
            new DelimiterBasedFrameDecoder(100, Delimiters.lineDelimiter: _*))
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

class ServerChannelConfigurationSpec extends SpecificationWithJUnit {
  "Server"  should {
    "close connection after max life time duration" in {

      val service = new Service[String, String] {
        def apply(request: String) = Future.value(request)
      }

      // create a server builder which will close connections in 2 seconds
      val address = new InetSocketAddress(0)
      val server = ServerBuilder()
        .codec(StringCodec)
        .bindTo(address)
        .name("FinagleServer")
        .hostConnectionMaxLifeTime(2 seconds)
        .build(service)

      val client: Service[String, String] = ClientBuilder()
        .codec(StringCodec)
        .hosts(server.localAddress)
        .hostConnectionLimit(1)
        .build()

      // Issue a request which is NOT newline-delimited. Server should close connection 
      // after waiting for 2 seconds for a new line
      val r = client("123")
      r() must throwA[ChannelClosedException]
      r.isThrow must beTrue
      server.close()
    }
  }

  "Server" should {
    "close connection after max idle time duration" in {

      val service = new Service[String, String] {
        def apply(request: String) = Future.value(request)
      }

      // create a server builder which will close connections in 2 seconds idle
      val address = new InetSocketAddress(0)
      val server = ServerBuilder()
        .codec(StringCodec)
        .bindTo(address)
        .name("FinagleServer")
        .hostConnectionMaxIdleTime(2 seconds)
        .build(service)

      val client: Service[String, String] = ClientBuilder()
        .codec(StringCodec)
        .hosts(server.localAddress)
        .hostConnectionLimit(1)
        .build()

      // Issue a request which is NOT newline-delimited. Server should close connection 
      // after waiting for 2 seconds for a new line
      val r = client("123")
      r() must throwA[ChannelClosedException]
      r.isThrow must beTrue
      server.close()
    }
  }
}