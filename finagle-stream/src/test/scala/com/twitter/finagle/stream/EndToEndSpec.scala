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

object EndToEndSpec extends Specification {
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

  "Streams" should {
    "work" in {
      val address = RandomSocket()
      val httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
      val httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
      val channelSource = new ChannelSource[ChannelBuffer]
      val server = ServerBuilder()
        .codec(new Stream)
        .bindTo(address)
        .build(new MyService(MyStreamResponse(httpResponse, channelSource)))
      val client = ClientBuilder()
        .codec(new Stream)
        .hosts(Seq(address))
        .build()

      val channel = client(httpRequest)(1.second).channel

      "writes from the server arrive on the client's channel" in {
        var result = ""
        val latch = new CountDownLatch(1)

        channel.closes.respond { _ =>
          Future { latch.countDown() }
        }

        channel.respond { channelBuffer =>
          Future {
            Thread.dumpStack
            result += channelBuffer.toString(Charset.defaultCharset)
          }
        }

        val futures: Seq[Future[Observer]] =
          channelSource.send(ChannelBuffers.wrappedBuffer("1".getBytes)) ++
          channelSource.send(ChannelBuffers.wrappedBuffer("2".getBytes)) ++
          channelSource.send(ChannelBuffers.wrappedBuffer("3".getBytes))

        channelSource.close()

        latch.await(1.second)
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