package com.twitter.finagle.stream

import com.twitter.concurrent._
import com.twitter.conversions.time._
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.Service
import com.twitter.util.{Future, RandomSocket, CountDownLatch}
import java.nio.charset.Charset
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.jboss.netty.handler.codec.http._
import org.specs.Specification

object PubSubSpec extends Specification {
  case class MyStreamResponse(
      httpResponse: HttpResponse,
      channel: Channel[ChannelBuffer])
      extends StreamResponse
  {
    def release() = ()
  }
  class MyService(response: StreamResponse) extends Service[HttpRequest, StreamResponse] {
    def apply(request: HttpRequest) = Future.value(response)
  }

  "PubSub" should {
    "work" in {
      val address = RandomSocket()
      val httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
      val httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
      val channelSource = new ChannelSource[ChannelBuffer]
      val server = ServerBuilder()
        .codec(new Stream)
        .bindTo(address)
        .name("PubSub")
        .build(new MyService(MyStreamResponse(httpResponse, channelSource)))
      val client = ClientBuilder()
        .codec(new Stream)
        .hosts(Seq(address))
        .hostConnectionLimit(2)
        .buildFactory()

      def makeChannel = for {
        client <- client.make()
        streamResponse <- client(httpRequest)
      } yield streamResponse.channel

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
