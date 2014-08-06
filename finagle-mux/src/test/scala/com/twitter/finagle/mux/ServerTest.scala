package com.twitter.finagle.mux

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.mux.Message.Treq
import com.twitter.finagle.mux.lease.exp.Lessor
import com.twitter.finagle.transport.{Transport, QueueTransport}
import com.twitter.util.{Future, Promise}
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.junit.runner.RunWith
import org.mockito.Mockito.{never, verify, when}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class ServerTest extends FunSuite with MockitoSugar {

  private class Ctx {
    val clientToServer = new AsyncQueue[ChannelBuffer]
    val serverToClient = new AsyncQueue[ChannelBuffer]
    val transport = new QueueTransport(writeq=serverToClient, readq=clientToServer)
    val service = mock[Service[ChannelBuffer, ChannelBuffer]]
    val lessor = mock[Lessor]
    val server = new ServerDispatcher(transport, service, true, lessor)
  }

  test("register/unregister with lessor") {
    val ctx = new Ctx
    import ctx._

    verify(lessor).register(server)
    verify(lessor, never()).unregister(server)
    clientToServer.fail(new Exception)
    verify(lessor).unregister(server)
  }

  test("propagate leases") {
    val ctx = new Ctx
    import ctx._

    val m = serverToClient.poll()
    assert(!m.isDefined)
    server.issue(123.milliseconds)
    assert(m.isDefined)
    assert(Message.decode(m()) === Message.Tlease(123.milliseconds))
  }

  test("does not leak pending on failures") {
    val ctx = new Ctx
    import ctx._

    val p = new Promise[ChannelBuffer]
    val svc = new Service[ChannelBuffer, ChannelBuffer] {
      def apply(request: ChannelBuffer): Future[ChannelBuffer] = p
    }

    val encodedMsg = Message.encode(
      Treq(tag = 9, traceId = None, ChannelBuffers.EMPTY_BUFFER))

    val trans = mock[Transport[ChannelBuffer, ChannelBuffer]]
    when(trans.onClose).thenReturn(new Promise[Throwable])
    when(trans.read())
      .thenReturn(Future.value(encodedMsg))
      .thenReturn(Future.never)

    val dispatcher = new ServerDispatcher(trans, svc, true, lessor)
    assert(dispatcher.npending() === 1)

    // fulfill the promise with a failure
    p.setException(new RuntimeException("welp"))

    assert(dispatcher.npending() === 0)
  }

}
