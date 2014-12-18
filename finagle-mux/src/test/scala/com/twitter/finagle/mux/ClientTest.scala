package com.twitter.finagle.mux

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.time._
import com.twitter.finagle.{Path, Status}
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.stats.{NullStatsReceiver, InMemoryStatsReceiver, StatsReceiver}
import com.twitter.finagle.transport.{Transport, QueueTransport}
import com.twitter.io.{Buf, Charsets}
import com.twitter.logging.{Logger, StringHandler, BareFormatter, Level}
import com.twitter.util.{Return, Throw, Time, TimeControl, Duration, Future}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ClientTest extends FunSuite {
  def leaseClient(
    fn: (
      ClientDispatcher,
      Transport[ChannelBuffer, ChannelBuffer],
      (Duration => Unit),
      TimeControl
    ) => Unit): Unit = {
    import Message._

    Time.withCurrentTimeFrozen { ctl =>
      val clientToServer = new AsyncQueue[ChannelBuffer]
      val serverToClient = new AsyncQueue[ChannelBuffer]
      val transport = new QueueTransport(writeq=clientToServer, readq=serverToClient)
      val client = new ClientDispatcher("test", transport, NullStatsReceiver)
      fn(
        client,
        transport,
        { duration: Duration => serverToClient.offer(encode(Tlease(duration))) },
        ctl
      )
    }
  }

  test("responds to leases") {
    import Message._

    leaseClient { (client, transport, issue, ctl) =>
      assert(transport.status == Status.Open)
      assert(client.status === Status.Open)
      issue(1.millisecond)
      ctl.advance(2.milliseconds)
      assert(Status.isBusy(client.status))
      val Status.Busy(until) = client.status
      assert(!until.isDefined)
      assert(transport.status == Status.Open)
      issue(Tlease.MaxLease)
      assert(until.poll === Some(Return.Unit))
      assert(client.status === Status.Open)
    }
  }

  class Client(sr: StatsReceiver) {
    val clientToServer = new AsyncQueue[ChannelBuffer]
    val serverToClient = new AsyncQueue[ChannelBuffer]
    val transport = new QueueTransport(writeq=clientToServer, readq=serverToClient)
    val client = new ClientDispatcher("test", transport, sr)

    def apply(req: Request): Future[Response] = client(req)
    def respond(rep: ChannelBuffer): Unit = serverToClient.offer(rep)
    def read(): Future[ChannelBuffer] = clientToServer.poll
  }

  test("nacks after sending rdrain") {
    Time.withCurrentTimeFrozen { ctl =>
      import Message._
      val buf = ChannelBuffers.copiedBuffer("OK", Charsets.Utf8)
      val client = new Client(NullStatsReceiver)

      val req = Request(Path.empty, ChannelBufferBuf(buf))
      val f = client(req)
      val Some(Return(treq)) = client.read().poll
      val Tdispatch(tag, contexts, _, _, _) = decode(treq)
      client.respond(encode(Tdrain(tag + 1)))

      val drained = client.read()

      val Some(Return(rdrain)) = drained.poll
      val Rdrain(newTag) = Message.decode(rdrain)

      val Status.Busy(doneBusy) = client.client.status
      assert(newTag === tag + 1)

      client.respond(encode(RdispatchOk(tag, contexts, 
        ChannelBuffers.copiedBuffer(buf.toString("UTF-8").reverse, Charsets.Utf8))))

      val Some(Return(result)) = f.poll
      assert(result === Response(Buf.Utf8("KO")))

      val f2 = client(req)
      val Some(Throw(nack)) = f2.poll
      assert(nack === RequestNackedException)

      assert(client.read().poll === None)
      
      // At this point, we're fully drained.
      assert(client.client.status === Status.Closed)
    }
  }
  
  test("Busy after sending rdrain") {
    Time.withCurrentTimeFrozen { ctl =>
      import Message._
      val buf = ChannelBuffers.copiedBuffer("OK", Charsets.Utf8)
      val client = new Client(NullStatsReceiver)

      val req = Request(Path.empty, ChannelBufferBuf(buf))

      val f = client(req)
      val Some(Return(treq)) = client.read().poll
      val Tdispatch(tag, _, _, _, _) = decode(treq)

      assert(client.client.status === Status.Open)
      client.respond(encode(Tdrain(tag + 1)))

      val drained = client.read()

      val Some(Return(rdrain)) = drained.poll
      val Rdrain(newTag) = Message.decode(rdrain)

      assert(newTag === tag + 1)
      assert(Status.isBusy(client.client.status))
    }
  }

  test("logs while draining") {
    Time.withCurrentTimeFrozen { ctl =>
      import Message._

      val log = Logger.get("")
      val handler = new StringHandler(BareFormatter, None)
      log.setLevel(Level.DEBUG)
      log.addHandler(handler)

      val buf = ChannelBuffers.copiedBuffer("OK", Charsets.Utf8)
      val req = Request(Path.empty, ChannelBufferBuf(buf))
      val inMemory = new InMemoryStatsReceiver

      var accumulated: String = ""
      val started = "Started draining a connection to test\n"
      val finished = "Finished draining a connection to test\n"

      val client1 = new Client(inMemory)
      val client2 = new Client(inMemory)

      assert(handler.get === accumulated)
      client2.respond(encode(Tdrain(1))) // drain, nothing outstanding

      accumulated += (started + finished)
      assert(handler.get === accumulated)

      val f1 = client1(req)
      val Some(Return(treq)) = client1.read().poll
      val Tdispatch(tag, contexts, _, _, _) = decode(treq)

      client1.respond(encode(Tdrain(1))) // drain, nothing outstanding

      accumulated += started
      assert(handler.get === accumulated)

      client1.respond(encode(
        RdispatchOk(tag, contexts, 
          ChannelBuffers.copiedBuffer(buf.toString("UTF-8").reverse, Charsets.Utf8))))
      // outstanding finished

      accumulated += finished
      assert(handler.get === accumulated)

      log.clearHandlers()
    }
  }
}
