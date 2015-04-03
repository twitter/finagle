package com.twitter.finagle.builder

import com.twitter.conversions.time._
import com.twitter.finagle.ChannelClosedException
import com.twitter.finagle.Service
import com.twitter.finagle.{Codec, CodecFactory}
import com.twitter.io.Charsets
import com.twitter.util.{Await, Future}
import java.net.{InetAddress, InetSocketAddress}
import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}
import org.jboss.netty.handler.codec.frame.{Delimiters, DelimiterBasedFrameDecoder}
import org.jboss.netty.handler.codec.string.{StringEncoder, StringDecoder}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
 * This Codec is a newline (\n) delimited line-based protocol. Here we re-use existing
 * encoders/decoders as provided by Netty. This codec allows us to make requests which
 * are incomplete due to missing newline (\n)
 */
object ServerChannelConfigCodec extends ServerChannelConfigCodec

class ServerChannelConfigCodec extends CodecFactory[String, String] {
  def server = Function.const {
    new Codec[String, String] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("line",
            new DelimiterBasedFrameDecoder(100, Delimiters.lineDelimiter: _*))
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

@RunWith(classOf[JUnitRunner])
class ServerChannelConfigurationTest extends FunSuite {
  val service = new Service[String, String] {
    def apply(request: String) = Future.value(request)
  }

  test("close connection after max life time duration") {
    // create a server builder which will close connections in 2 seconds
    val address = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val server = ServerBuilder()
      .codec(ServerChannelConfigCodec)
      .bindTo(address)
      .name("FinagleServer")
      .hostConnectionMaxLifeTime(2 seconds)
      .build(service)

    val client: Service[String, String] = ClientBuilder()
      .codec(ServerChannelConfigCodec)
      .daemon(true) // don't create an exit guard
      .hosts(server.boundAddress)
      .hostConnectionLimit(1)
      .build()

    // Issue a request which is NOT newline-delimited. Server should close connection
    // after waiting for 2 seconds for a new line
    intercept[ChannelClosedException] {
      Await.result(client("123"), 15.seconds)
    }
    server.close()
  }

  test("close connection after max idle time duration") {
    // create a server builder which will close idle connections in 2 seconds
    val address = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val server = ServerBuilder()
      .codec(ServerChannelConfigCodec)
      .bindTo(address)
      .name("FinagleServer")
      .hostConnectionMaxIdleTime(2 seconds)
      .build(service)

    val client: Service[String, String] = ClientBuilder()
      .codec(ServerChannelConfigCodec)
      .daemon(true) // don't create an exit guard
      .hosts(server.boundAddress)
      .hostConnectionLimit(1)
      .build()

    // Issue a request which is NOT newline-delimited. Server should close connection
    // after waiting for 2 seconds for a new line
    intercept[ChannelClosedException] {
      Await.result(client("123"), 5.seconds)
    }
    server.close()
  }
}
