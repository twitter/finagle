package com.twitter.finagle.stream

import com.twitter.concurrent._
import com.twitter.conversions.time._
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.Service
import com.twitter.util.{Future, RandomSocket, CountDownLatch, Promise, Return}
import java.nio.charset.Charset
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.jboss.netty.handler.codec.http._
import org.specs.Specification

object EndToEndSpec extends Specification {
  case class MyStreamResponse(
      httpResponse: HttpResponse,
      messages: Offer[ChannelBuffer],
      error: Offer[Throwable])
      extends StreamResponse
  {
    val released = new Promise[Unit]
    def release() = released.updateIfEmpty(Return(()))
  }

  class MyService(response: StreamResponse) extends Service[HttpRequest, StreamResponse] {
    def apply(request: HttpRequest) = Future.value(response)
  }

  "Streams" should {
    "work" in {
      val address = RandomSocket()
      val httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
      val httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
      val messages = new Broker[ChannelBuffer]
      val error = new Broker[Throwable]
      val serverRes = MyStreamResponse(httpResponse, messages.recv, error.recv)
      val server = ServerBuilder()
        .codec(new Stream)
        .bindTo(address)
        .name("Streams")
        .build(new MyService(serverRes))
      val clientFactory = ClientBuilder()
        .codec(new Stream)
        .hosts(Seq(address))
        .hostConnectionLimit(1)
        .buildFactory()
      val client = clientFactory.make()()

      doAfter {
        client.release()
        clientFactory.close()
        server.close()
      }

      val clientRes = client(httpRequest)(1.second)

      "writes from the server arrive on the client's channel" in {
        var result = ""
        val latch = new CountDownLatch(1)
        (clientRes.error?) ensure {
          Future { latch.countDown() }
        }

        clientRes.messages foreach { channelBuffer =>
          Future {
            result += channelBuffer.toString(Charset.defaultCharset)
          }
        }

        messages ! ChannelBuffers.wrappedBuffer("1".getBytes)
        messages ! ChannelBuffers.wrappedBuffer("2".getBytes)
        messages ! ChannelBuffers.wrappedBuffer("3".getBytes)
        error ! EOF

        latch.within(1.second)
        result mustEqual "123"
      }

      "writes from the server are queued before the client responds" in {
        messages ! ChannelBuffers.wrappedBuffer("1".getBytes)
        messages ! ChannelBuffers.wrappedBuffer("2".getBytes)
        messages ! ChannelBuffers.wrappedBuffer("3".getBytes)

        val latch = new CountDownLatch(3)
        var result = ""
        clientRes.messages foreach { channelBuffer =>
          Future {
            result += channelBuffer.toString(Charset.defaultCharset)
            latch.countDown()
          }
        }

        latch.within(1.second)
        error ! EOF
        result mustEqual "123"
      }
    }
  }
}
