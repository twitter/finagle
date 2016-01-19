package com.twitter.finagle.mux

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.time._
import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.stats.{NullStatsReceiver, InMemoryStatsReceiver, StatsReceiver}
import com.twitter.finagle.transport.{Transport, QueueTransport}
import com.twitter.finagle.{Failure, Dtab, Path, Status}
import com.twitter.io.Charsets
import com.twitter.util.{Await, Return, Throw, Time, TimeControl, Duration, Future}
import java.nio.charset.Charset
import org.jboss.netty.buffer.ChannelBuffers
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
private class ClientSessionTest extends FunSuite {

  private class Ctx {
    val clientToServer = new AsyncQueue[Message]
    val serverToClient = new AsyncQueue[Message]

    val transport = new QueueTransport(writeq=clientToServer, readq=serverToClient)

    val stats = new InMemoryStatsReceiver
    val session = new ClientSession(transport, FailureDetector.NullConfig, "test", stats)

    def send(msg: Message) = {
      Await.result(session.write(msg))
      Await.result(clientToServer.poll())
    }

    def recv(msg: Message) = {
      serverToClient.offer(msg)
      Await.result(session.read())
    }
  }

  test("responds to leases") {
    Time.withCurrentTimeFrozen { ctl =>
      val ctx = new Ctx
      import ctx._

      assert(transport.status == Status.Open)
      assert(session.status === Status.Open)
      recv(Message.Tlease(1.millisecond))
      ctl.advance(2.milliseconds)
      assert(session.status == Status.Busy)
      assert(transport.status == Status.Open)
      recv(Message.Tlease(Message.Tlease.MaxLease))
      assert(session.status === Status.Open)
    }
  }

  test("drains requests") {
    Time.withCurrentTimeFrozen { ctl =>
      val ctx = new Ctx
      import ctx._

      val buf = ChannelBuffers.copiedBuffer("OK", Charsets.Utf8)
      val req = Message.Tdispatch(2, Seq.empty, Path.empty, Dtab.empty, buf)

      // 2 outstanding req, it's okay to use the same tag
      // since the session doesn't verify our tags
      send(req)
      send(req)

      val tag = 5
      recv(Message.Tdrain(tag))
      assert(Await.result(clientToServer.poll()) == Message.Rdrain(tag))
      assert(session.status == Status.Busy)

      session.write(req).poll match {
        case Some(Throw(f: Failure)) =>
          assert(f.isFlagged(Failure.Restartable))
          assert(f.getMessage == "The request was Nacked by the server")
        case _ => fail()
      }

      val rep = Message.RdispatchOk(2, Seq.empty, buf)
      recv(rep)
      assert(session.status == Status.Busy)
      recv(rep)
      assert(session.status == Status.Closed)

      assert(stats.counters(Seq("drained")) == 1)
      assert(stats.counters(Seq("draining")) == 1)
    }
  }

  test("pings") {
    val ctx = new Ctx
    import ctx._

    val ping0 = session.ping()
    assert(!ping0.isDefined)

    session.ping().poll match {
      case Some(Throw(f: Failure)) =>
        assert(f.getMessage == "A ping is already oustanding on this session.")
      case _ => fail()
    }

    recv(Message.Rping(Message.PingTag))
    assert(ping0.isDefined)

    val ping1 = session.ping()
    recv(Message.Rping(Message.PingTag))
    assert(ping1.isDefined)
  }
}