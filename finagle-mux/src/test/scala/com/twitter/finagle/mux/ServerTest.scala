package com.twitter.finagle.mux

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.mux.lease.exp.Lessor
import com.twitter.finagle.transport.QueueTransport
import org.jboss.netty.buffer.ChannelBuffer
import org.junit.runner.RunWith
import org.mockito.Mockito.{verify, never}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import com.twitter.conversions.time._

@RunWith(classOf[JUnitRunner])
class ServerTest extends FunSuite with MockitoSugar {
  class Ctx {
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
}
