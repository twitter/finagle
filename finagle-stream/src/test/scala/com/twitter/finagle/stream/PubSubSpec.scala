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

object PubSubSpec extends Specification {
  class MyService(topic: ChannelSource[ChannelBuffer]) extends Service[HttpRequest, Channel[ChannelBuffer]] {
    def apply(request: HttpRequest) = {
      Future.value(topic)
    }
  }

  "PubSub" should {
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
        .buildFactory()

      val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")

      def makeChannel = for {
        client <- client.make()
        channel <- client(request)
      } yield channel

      // these two channels represent two TCP connections to the server
      val channel1 = makeChannel(1.second)
      val channel2 = makeChannel(1.second)

      val result = new java.util.concurrent.ConcurrentLinkedQueue[String]
      val latchForChannel1 = new CountDownLatch(2)
      val latchForChannel2 = new CountDownLatch(1)
      val o1 = channel1.respond { m =>
        Future {
          result add m.toString(Charset.defaultCharset)
          latchForChannel1.countDown()
        }
      }
      val o2 = channel2.respond { m =>
        Future {
          result add m.toString(Charset.defaultCharset)
          latchForChannel2.countDown()
        }
      }

      channelSource.send(ChannelBuffers.wrappedBuffer("1".getBytes))
      latchForChannel2.await(1.second) mustBe true
      o2.dispose()
      channelSource.send(ChannelBuffers.wrappedBuffer("2".getBytes))
      latchForChannel1.await(1.second) mustBe true
      result.poll() mustEqual "1"
      result.poll() mustEqual "1"
      result.poll() mustEqual "2"
    }
  }

}
