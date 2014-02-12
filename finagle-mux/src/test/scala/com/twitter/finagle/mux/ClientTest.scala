package com.twitter.finagle.mux

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.time._
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.transport.QueueTransport
import com.twitter.util.Time
import org.jboss.netty.buffer.ChannelBuffer
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
}
