package com.twitter.finagle.mux

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.time._
import com.twitter.io.Charsets
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.transport.QueueTransport
import com.twitter.util.{Return, Throw, Time}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ClientTest extends FunSuite {
  test("responds to leases") {
    Time.withCurrentTimeFrozen { ctl =>
      import Message._

      val clientToServer = new AsyncQueue[ChannelBuffer]
      val serverToClient = new AsyncQueue[ChannelBuffer]
      val transport = new QueueTransport(writeq=clientToServer, readq=serverToClient)
      val client = new ClientDispatcher(transport, NullStatsReceiver)

      assert(transport.isOpen)
      assert(client.isAvailable)
      serverToClient.offer(encode(Tlease(1.millisecond)))
      ctl.advance(2.milliseconds)
      assert(!client.isActive && transport.isOpen)
      serverToClient.offer(encode(Tlease(Tlease.MaxLease)))
      assert(client.isActive)
    }
  }

  test("nacks after sending rdrain") {
    Time.withCurrentTimeFrozen { ctl =>
      import Message._
      val buf = ChannelBuffers.copiedBuffer("OK", Charsets.Utf8)
      val clientToServer = new AsyncQueue[ChannelBuffer]
      val serverToClient = new AsyncQueue[ChannelBuffer]
      val transport = new QueueTransport(writeq=clientToServer, readq=serverToClient)
      val client = new ClientDispatcher(transport, NullStatsReceiver)

      val f = client(buf)
      val Some(Return(treq)) = clientToServer.poll.poll
      val Tdispatch(tag, contexts, dst, dtab, req) = decode(treq)
      serverToClient.offer(encode(Tdrain(tag + 1)))

      val drained = clientToServer.poll

      val Some(Return(rdrain)) = drained.poll
      val Rdrain(newTag) = Message.decode(rdrain)

      assert(!client.isActive)
      assert(newTag === tag + 1)

      serverToClient.offer(encode(RdispatchOk(tag, contexts, ChannelBuffers.copiedBuffer(req.toString("UTF-8").reverse, Charsets.Utf8))))

      val Some(Return(result)) = f.poll
      assert(result === ChannelBuffers.copiedBuffer(buf.toString("UTF-8").reverse, Charsets.Utf8))

      val f2 = client(buf)
      val Some(Throw(nack)) = f2.poll
      assert(nack === RequestNackedException)

      assert(clientToServer.poll.poll === None)
    }
  }

  test("isAvailable after sending rdrain") {
    Time.withCurrentTimeFrozen { ctl =>
      import Message._
      val buf = ChannelBuffers.copiedBuffer("OK", Charsets.Utf8)
      val clientToServer = new AsyncQueue[ChannelBuffer]
      val serverToClient = new AsyncQueue[ChannelBuffer]
      val transport = new QueueTransport(writeq=clientToServer, readq=serverToClient)
      val client = new ClientDispatcher(transport, NullStatsReceiver)

      val f = client(buf)
      val Some(Return(treq)) = clientToServer.poll.poll
      val Tdispatch(tag, contexts, dst, dtab, req) = decode(treq)
      serverToClient.offer(encode(Tdrain(tag + 1)))

      val drained = clientToServer.poll

      val Some(Return(rdrain)) = drained.poll
      val Rdrain(newTag) = Message.decode(rdrain)

      assert(newTag === tag + 1)
      assert(client.isAvailable)
    }
  }
}
