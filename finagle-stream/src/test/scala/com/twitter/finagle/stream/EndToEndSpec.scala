package com.twitter.finagle.stream

import org.specs.Specification
import com.twitter.finagle.Service
import com.twitter.concurrent._
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import org.jboss.netty.handler.codec.http.{HttpMethod, HttpVersion, DefaultHttpRequest, HttpRequest}
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import com.twitter.util.{CountDownLatch, Future, RandomSocket}
import com.twitter.conversions.time._
import java.nio.charset.Charset

object EndToEndSpec extends Specification {
  class MyService(topic: Topic[ChannelBuffer]) extends Service[HttpRequest, Channel[ChannelBuffer]] {
    def apply(request: HttpRequest) = Future.value(topic)
  }

  "Streams" should {
    "work" in {
      val address = RandomSocket()
      val topic = new Topic[ChannelBuffer]
      val server = ServerBuilder()
        .codec(new Stream)
        .bindTo(address)
        .build(new MyService(topic))
      val client = ClientBuilder()
        .codec(new Stream)
        .hosts(Seq(address))
        .build()

      val channel = client(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/"))(1.second)
      topic.send(ChannelBuffers.wrappedBuffer("1".getBytes))
      topic.send(ChannelBuffers.wrappedBuffer("2".getBytes))
      topic.send(ChannelBuffers.wrappedBuffer("3".getBytes))
      topic.close()

      val latch = new CountDownLatch(1)
      var result = ""
      channel.receive {
        case Value(channelBuffer) =>
          result += channelBuffer.toString(Charset.defaultCharset)
        case End =>
          latch.countDown()
      }
      latch.within(1.second)
      result mustEqual "123"
    }
  }
}