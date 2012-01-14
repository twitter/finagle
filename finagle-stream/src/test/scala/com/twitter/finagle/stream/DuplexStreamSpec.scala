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
  class SimpleService extends Service[DuplexStreamHandle, Offer[ChannelBuffer]] {
    var handle: Promise[DuplexStreamHandle] = new Promise[DuplexStreamHandle]
    var input: Promise[Offer[ChannelBuffer]] = new Promise[Offer[ChannelBuffer]]
    var output: Broker[ChannelBuffer] = new Broker[ChannelBuffer]

    def apply(h: DuplexStreamHandle) = {
      handle() = Return(h)
      input() = Return(h.messages)
      Future.value(output.recv)
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
        .codec(DuplexStreamServerCodec())
        .bindTo(address)
        .name("SimpleService")
        .build(service)

      val factory = ClientBuilder()
        .codec(DuplexStreamClientCodec())
        .hosts(Seq(address))
        .hostConnectionLimit(1)
        .buildFactory()

      val client = factory.make()()

      val outbound = new Broker[ChannelBuffer]
      val handle = client(outbound.recv)()
      val inbound = handle.messages

      "receive and reverse" in {
        service.input.isDefined mustEqual true
        val input = service.input()
        input() foreach { str =>
          service.output.send(bufferToString(str).reverse)()
        }

        outbound.send("hello")()
        bufferToString(inbound()()) mustEqual "olleh"
      }

      "send two consequitive messages and receive them" in {
        var count = 0

        service.input.isDefined mustEqual true
        val input = service.input()

        input foreach { _ =>
          count += 1
          if (count == 2) {
            service.output.send("done")()
          }
        }
        outbound.send("hello")()
        outbound.send("world")()
        bufferToString(inbound()()) mustEqual "done"
        count mustEqual 2
      }

      "server closes when client initiates close" in {
        service.handle().onClose.isDefined mustEqual false
        handle.close()
        service.handle().onClose.get(1.seconds) mustEqual Return(())
      }

      "client closes when server initiates close" in {
        handle.onClose.isDefined mustEqual false
        service.handle().close()
        handle.onClose.get(1.seconds) mustEqual Return(())
      }
    }
  }
}
