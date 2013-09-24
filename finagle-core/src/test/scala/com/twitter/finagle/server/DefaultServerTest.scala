package com.twitter.finagle.server

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle._
import com.twitter.finagle.transport.{QueueTransport, Transport}
import com.twitter.util._
import com.twitter.finagle.stats.StatsReceiver
import java.net.{SocketAddress, InetAddress, InetSocketAddress}
import org.junit.runner.RunWith
import org.mockito.Matchers.anyObject
import org.mockito.Mockito.when
import org.scalatest.FunSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class DefaultServerTest extends FunSpec with MockitoSugar {
  describe("DefaultServer") {
    it("should successfully add sourcedexception") {
      val name = "name"

      val qIn = new AsyncQueue[Try[Int]]()
      val qOut = new AsyncQueue[Try[Int]]()
      val listener = new FakeListener(qIn, qOut)
      val clientTransport = new QueueTransport(qOut, qIn)

      val serviceTransport: (Transport[Try[Int], Try[Int]], Service[Try[Int], Try[Int]]) => Closable = { case (transport, service) =>
          val f = transport.read() flatMap { num =>
            service(num)
          } respond { result =>
            transport.write(result.flatten)
          }
          service
      }

      val server: Server[Try[Int], Try[Int]] = DefaultServer[Try[Int], Try[Int], Try[Int], Try[Int]](name, listener, serviceTransport)

      val socket = new InetSocketAddress(InetAddress.getLocalHost(), 8080).asInstanceOf[SocketAddress]
      val factory = ServiceFactory.const(Service.mk[Try[Int], Try[Int]] { num =>
        Future.exception(new SourcedException{})
      })

      val listeningServer: ListeningServer = server.serve(socket, factory)

      Await.result(clientTransport.write(Return(3)))
      val e = intercept[SourcedException] {
        Await.result(clientTransport.read())()
      }
      assert(e.serviceName === name)
    }
  }
}

class FakeListener(qIn: AsyncQueue[Try[Int]], qOut: AsyncQueue[Try[Int]]) extends Listener[Try[Int], Try[Int]]{
  override def listen(addr: SocketAddress)(serveTransport: Transport[Try[Int], Try[Int]] => Unit): ListeningServer = {
    val transport = new QueueTransport(qIn, qOut)
    serveTransport(transport)
    NullServer
  }
}
