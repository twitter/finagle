package com.twitter.finagle.stream

import com.twitter.concurrent._
import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.util.{Future, Promise, Return}
import java.net.InetSocketAddress
import java.nio.charset.Charset
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.specs.SpecificationWithJUnit

class DuplexStreamSpec extends SpecificationWithJUnit {
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
    buf.toString(Charset.forName("UTF-8"))
  }

  "SimpleService" should {
    "work" in {
      val service = new SimpleService

      val server = ServerBuilder()
        .codec(DuplexStreamServerCodec())
        .bindTo(new InetSocketAddress(0))
        .name("SimpleService")
        .build(service)
      val address = server.localAddress

      val factory = ClientBuilder()
        .codec(DuplexStreamClientCodec())
        .hosts(Seq(address))
        .hostConnectionLimit(1)
        .buildFactory()

      val client = factory()()

      val outbound = new Broker[ChannelBuffer]
      val handle = client(outbound.recv)()
      val inbound = handle.messages

      "receive and reverse" in {
        service.input.isDefined mustEqual true
        val input = service.input()
        input.sync() foreach { str =>
          service.output.send(bufferToString(str).reverse).sync()
        }

        outbound.send("hello").sync()
        bufferToString(inbound.sync()()) mustEqual "olleh"
      }

      "send two consequitive messages and receive them" in {
        var count = 0

        service.input.isDefined mustEqual true
        val input = service.input()

        input foreach { _ =>
          count += 1
          if (count == 2) {
            service.output.send("done").sync()
          }
        }
        outbound.send("hello").sync()
        outbound.send("world").sync()
        bufferToString(inbound.sync()()) mustEqual "done"
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
