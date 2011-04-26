package com.twitter.finagle.stream

import org.specs.Specification
import com.twitter.finagle.Service
import com.twitter.concurrent._
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import org.jboss.netty.handler.codec.http.{HttpMethod, HttpVersion, DefaultHttpRequest, HttpRequest}
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import com.twitter.util.{Future, RandomSocket, CountDownLatch}
import com.twitter.conversions.time._
import com.twitter.concurrent._
import com.twitter.conversions.time._
import java.nio.charset.Charset

object EndToEndSpec extends Specification {
  class MyService(topic: ChannelSource[ChannelBuffer]) extends Service[HttpRequest, Channel[ChannelBuffer]] {
    def apply(request: HttpRequest) = Future.value(topic)
  }

  "Streams" should {
    "work" in {
      val address = RandomSocket()
      val channelSource = new ChannelSource[ChannelBuffer]
      val server = ServerBuilder()
        .codec(new Stream)
        .bindTo(address)
        .build(new MyService(channelSource))
      val client = ClientBuilder()
        .codec(new Stream)
        .hosts(Seq(address))
        .hostConnectionLimit(1)
        .build()

      val channel = client(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/"))(1.second)

      "writes from the server arrive on the client's channel" in {
        var result = ""
        channel.respond { channelBuffer =>
          Future {
            result += channelBuffer.toString(Charset.defaultCharset)
          }
        }

        channelSource.send(ChannelBuffers.wrappedBuffer("1".getBytes))
        channelSource.send(ChannelBuffers.wrappedBuffer("2".getBytes))
        channelSource.send(ChannelBuffers.wrappedBuffer("3".getBytes))

        channelSource.close()

        result mustEqual "123"
      }

      "writes from the server are queued before the client responds" in {
        channelSource.send(ChannelBuffers.wrappedBuffer("1".getBytes))
        channelSource.send(ChannelBuffers.wrappedBuffer("2".getBytes))
        channelSource.send(ChannelBuffers.wrappedBuffer("3".getBytes))

        val latch = new CountDownLatch(3)
        var result = ""
        channel.respond { channelBuffer =>
          Future {
            result += channelBuffer.toString(Charset.defaultCharset)
            latch.countDown()
          }
        }

        latch.await(1.second)
        channelSource.close()
        result mustEqual "123"
      }
    }
  }
}
