package com.twitter.finagle.stream

import org.specs.Specification
import com.twitter.finagle.Service
import com.twitter.concurrent._
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import org.jboss.netty.handler.codec.http.{HttpMethod, HttpVersion, DefaultHttpRequest, HttpRequest}
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import com.twitter.util.{Future, RandomSocket, CountDownLatch, Promise, Return}
import com.twitter.conversions.time._
import com.twitter.concurrent._
import com.twitter.conversions.time._
import java.nio.charset.Charset


object DuplexStreamSpec extends Specification {
  class SimpleService extends Service[Channel[ChannelBuffer], Channel[ChannelBuffer]] {
    var input: Promise[Channel[ChannelBuffer]] = new Promise[Channel[ChannelBuffer]]
    var output: ChannelSource[ChannelBuffer] = new ChannelSource[ChannelBuffer]

    def apply(channel: Channel[ChannelBuffer]) = {
      input() = Return(channel)
      Future.value(output)
    }
  }

  implicit def stringToBuffer(str: String): ChannelBuffer = {
    ChannelBuffers.wrappedBuffer(str.getBytes)
  }

  implicit def bufferToString(buf: ChannelBuffer): String = {
    buf.toString(Charset.defaultCharset)
  }

  "SimpleService" should {
    "work" in {
      val address = RandomSocket()
      val service = new SimpleService

      val server = ServerBuilder()
        .codec(new DuplexStreamCodec(true))
        .bindTo(address)
        .name("SimpleService")
        .build(service)

      val factory = ClientBuilder()
        .codec(new DuplexStreamCodec(true))
        .hosts(Seq(address))
        .hostConnectionLimit(1)
        .buildFactory()

      val client = factory.make()()

      val outbound = new ChannelSource[ChannelBuffer]
      val inbound = client(outbound)()

      "receive and reverse" in {
        val first = inbound.first
        service.input().first.foreach { str =>
          service.output send bufferToString(str).reverse
        }

        outbound send "hello"
        bufferToString(first()) mustEqual "olleh"
      }

      "send two consequitive messages and receive them" in {
        var count = 0
        val first = inbound.first

        service.input().respond { _ =>
          count += 1
          if (count == 2) {
            service.output.send("done")
          }
          Future.Unit
        }
        (outbound send "hello")()
        (outbound send "world")()
        bufferToString(first()) mustEqual "done"
        count mustEqual 2
      }
    }
  }
}
