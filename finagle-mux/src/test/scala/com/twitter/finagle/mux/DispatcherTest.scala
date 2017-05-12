package com.twitter.finagle.mux

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.time._
import com.twitter.finagle.liveness.FailureDetector
import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.transport.QueueTransport
import com.twitter.finagle.{Path, Service}
import com.twitter.io.Buf
import com.twitter.util.{Await, Promise}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DispatcherTest extends FunSuite
  with Eventually
  with IntegrationPatience {

  test("Discard request properly sent") {
    @volatile var handled = false
    val p = Promise[Response]()
    p.setInterruptHandler { case t: Throwable =>
      handled = true
    }

    val svc = Service.mk[Request, Response](_ => p)

    val q0, q1 = new AsyncQueue[Message]
    val clientTrans = new QueueTransport[Message, Message](q0, q1)
    val serverTrans = new QueueTransport[Message, Message](q1, q0)

    val server = ServerDispatcher.newRequestResponse(serverTrans, svc)
    val session = new ClientSession(
      clientTrans, FailureDetector.NullConfig, "test", NullStatsReceiver)
    val client = ClientDispatcher.newRequestResponse(session)

    val f = client(Request(Path.empty, Buf.Empty))
    assert(!f.isDefined)
    assert(!p.isDefined)
    f.raise(new Exception())
    eventually { assert(handled) }
    Await.ready(server.close().join(client.close()), 5.seconds)
  }
}
