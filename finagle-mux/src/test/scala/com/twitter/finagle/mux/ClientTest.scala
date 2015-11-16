package com.twitter.finagle.mux

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.time._
import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.stats.{NullStatsReceiver, InMemoryStatsReceiver, StatsReceiver}
import com.twitter.finagle.transport.{Transport, QueueTransport}
import com.twitter.finagle.{Path, Status, Failure}
import com.twitter.io.{Buf, Charsets}
import com.twitter.logging.{Logger, StringHandler, BareFormatter, Level}
import com.twitter.util.{Return, Throw, Time, TimeControl, Duration, Future}
import java.nio.charset.Charset
import org.jboss.netty.buffer.ChannelBuffers
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ClientTest extends FunSuite {
  private[this] def leaseClient(
    fn: (
      ClientDispatcher,
      Transport[Message, Message],
      (Duration => Unit),
      TimeControl
    ) => Unit): Unit = {
    import Message._

    Time.withCurrentTimeFrozen { ctl =>
      val clientToServer = new AsyncQueue[Message]
      val serverToClient = new AsyncQueue[Message]
      val transport = new QueueTransport(writeq=clientToServer, readq=serverToClient)
      val client = new ClientDispatcher("test", transport, NullStatsReceiver, FailureDetector.NullConfig)
      fn(
        client,
        transport,
        { duration: Duration => serverToClient.offer(Tlease(duration)) },
        ctl
      )
    }
  }

  test("responds to leases") {
    import Message._

    leaseClient { (client, transport, issue, ctl) =>
      assert(transport.status == Status.Open)
      assert(client.status == Status.Open)
      issue(1.millisecond)
      ctl.advance(2.milliseconds)
      assert(client.status == Status.Busy)
      assert(transport.status == Status.Open)
      issue(Tlease.MaxLease)
      assert(client.status == Status.Open)
    }
  }

  private[this] class Client(sr: StatsReceiver) {
    val clientToServer = new AsyncQueue[Message]
    val serverToClient = new AsyncQueue[Message]
    val transport = new QueueTransport(writeq=clientToServer, readq=serverToClient)
    val client = new ClientDispatcher("test", transport, sr, FailureDetector.NullConfig)

    def apply(req: Request): Future[Response] = client(req)
    def respond(rep: Message): Unit = serverToClient.offer(rep)
    def read(): Future[Message] = clientToServer.poll
  }

  test("nacks after sending rdrain") {
    Time.withCurrentTimeFrozen { ctl =>
      val buf = ChannelBuffers.copiedBuffer("OK", Charsets.Utf8)
      val client = new Client(NullStatsReceiver)

      val req = Request(Path.empty, ChannelBufferBuf.Owned(buf))
      val f = client(req)
      val Some(Return(treq)) = client.read().poll
      val Message.Tdispatch(tag, contexts, _, _, _) = treq
      client.respond(Message.Tdrain(tag + 1))

      val drained = client.read()

      val Some(Return(rdrain)) = drained.poll
      val Message.Rdrain(newTag) = rdrain

      assert(client.client.status == Status.Busy)
      assert(newTag == tag + 1)

      client.respond(Message.RdispatchOk(tag, contexts,
        ChannelBuffers.copiedBuffer(buf.toString(Charset.forName("UTF-8")).reverse, Charsets.Utf8)))

      val Some(Return(result)) = f.poll
      assert(result == Response(Buf.Utf8("KO")))

      val f2 = client(req)
      f2.poll match {
        case Some(Throw(f: Failure)) =>
          assert(f.isFlagged(Failure.Restartable))
          assert(f.getMessage == "The request was Nacked by the server")
        case _ => fail()
      }

      assert(client.read().poll == None)

      // At this point, we're fully drained.
      assert(client.client.status == Status.Closed)
    }
  }

  test("Busy after sending rdrain") {
    Time.withCurrentTimeFrozen { ctl =>
      val buf = ChannelBuffers.copiedBuffer("OK", Charsets.Utf8)
      val client = new Client(NullStatsReceiver)

      val req = Request(Path.empty, ChannelBufferBuf.Owned(buf))

      val f = client(req)
      val Some(Return(treq)) = client.read().poll
      val Message.Tdispatch(tag, _, _, _, _) = treq

      assert(client.client.status == Status.Open)
      client.respond(Message.Tdrain(tag + 1))

      val drained = client.read()

      val Some(Return(rdrain)) = drained.poll
      val Message.Rdrain(newTag) = rdrain

      assert(newTag == tag + 1)
      assert(client.client.status == Status.Busy)
    }
  }

  test("instrument request draining") {
    Time.withCurrentTimeFrozen { ctl =>
      val buf = ChannelBuffers.copiedBuffer("OK", Charsets.Utf8)
      val req = Request(Path.empty, ChannelBufferBuf.Owned(buf))
      val inMemory = new InMemoryStatsReceiver

      def drained = inMemory.counters(Seq("drained"))
      def draining = inMemory.counters(Seq("draining"))

      val client1 = new Client(inMemory)
      val client2 = new Client(inMemory)

      client2.respond(Message.Tdrain(1)) // drain, nothing outstanding
      assert(drained == 1)
      assert(draining == 1)

      client1(req)
      val Some(Return(treq)) = client1.read().poll
      val Message.Tdispatch(tag, contexts, _, _, _) = treq

      client1.respond(Message.Tdrain(1)) // drain, nothing outstanding
      assert(drained == 1)
      assert(draining == 2)


      client1.respond(
        Message.RdispatchOk(tag, contexts,
          ChannelBuffers.copiedBuffer(buf.toString(Charset.forName("UTF-8")).reverse, Charsets.Utf8)))
      // outstanding finished
      assert(drained == 2)
      assert(draining == 2)
    }
  }
}
