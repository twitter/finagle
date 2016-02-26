package com.twitter.finagle.server

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.client.{Bridge, DefaultClient}
import com.twitter.finagle.dispatch._
import com.twitter.finagle.transport.{QueueTransport, Transport}
import com.twitter.util._
import com.twitter.finagle.stats.StatsReceiver
import java.net.{SocketAddress, InetAddress, InetSocketAddress}
import org.junit.runner.RunWith
import org.mockito.Matchers.any
import org.mockito.Mockito.{verify, when}
import org.scalatest.FunSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class DefaultServerTest extends FunSpec with MockitoSugar {
  describe("DefaultServer") {
    val name = "name"

    it("should successfully add sourcedexception") {
      val qIn = new AsyncQueue[Try[Int]]()
      val qOut = new AsyncQueue[Try[Int]]()
      val listener = new FakeListener[Try[Int]](qIn, qOut)
      val clientTransport = new QueueTransport(qOut, qIn)

      val serviceTransport: (Transport[Try[Int], Try[Int]], Service[Try[Int], Try[Int]]) => Closable = {
        case (transport, service) =>
          val f = transport.read() flatMap { num =>
            service(num)
          } respond { result =>
            transport.write(result.flatten)
          }
          service
      }

      val server: Server[Try[Int], Try[Int]] = DefaultServer[Try[Int], Try[Int], Try[Int], Try[Int]](name, listener, serviceTransport)

      val socket = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
      val factory = ServiceFactory.const(Service.mk[Try[Int], Try[Int]] { num =>
        Future.exception(new SourcedException {})
      })

      val listeningServer: ListeningServer = server.serve(socket, factory)

      Await.result(clientTransport.write(Return(3)))
      val e = intercept[SourcedException] {
        Await.result(clientTransport.read())()
      }
      assert(e.serviceName == name)
    }

    it("should close ServiceFactory and any dispatcher(s) on closure") {
      val qIn = new AsyncQueue[Try[Int]]()
      val qOut = new AsyncQueue[Try[Int]]()
      val listener = new FakeListener[Try[Int]](qIn, qOut)
      val clientTransport = new QueueTransport(qOut, qIn)

      val mockConnHandle = mock[Closable]
      when(mockConnHandle.close(any[Time])) thenReturn Future.Done

      val serviceTransport: (Transport[Try[Int], Try[Int]], Service[Try[Int], Try[Int]]) => Closable =
        (_, _) => mockConnHandle

      val server: Server[Try[Int], Try[Int]] = DefaultServer[Try[Int], Try[Int], Try[Int], Try[Int]](name, listener, serviceTransport)

      val socket = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
      val factory = mock[ServiceFactory[Try[Int], Try[Int]]]
      val service = Service.mk[Try[Int], Try[Int]] { Future.value }
      when(factory(any[ClientConnection])) thenReturn Future.value(service)
      when(factory.close(any[Time])) thenReturn Future.Done
      val listeningServer: ListeningServer = server.serve(socket, factory)

      assert(clientTransport.write(Return(3)).isDefined == true)

      val deadline = 1.second.fromNow
      Await.result(listeningServer.close(deadline))
      verify(factory).close(deadline)
      verify(mockConnHandle).close(deadline)
    }

    it("should drain outstanding request on closure") {
      val qIn = new AsyncQueue[Try[Int]]()
      val qOut = new AsyncQueue[Try[Int]]()
      val listener = new FakeListener[Try[Int]](qIn, qOut)
      val clientTransport = new QueueTransport(qOut, qIn)

      val mockConnHandle = mock[Closable]
      when(mockConnHandle.close(any[Time])) thenReturn Future.Done

      val serviceTransport: (Transport[Try[Int], Try[Int]], Service[Try[Int], Try[Int]]) => Closable =
        new SerialServerDispatcher(_, _)

      val server: Server[Try[Int], Try[Int]] = DefaultServer[Try[Int], Try[Int], Try[Int], Try[Int]](name, listener, serviceTransport)
      val socket = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)

      val p = Promise[Try[Int]]
      val svc = Service.mk[Try[Int], Try[Int]] { _ => p }
      val factory = ServiceFactory.const(svc)
      val listeningServer: ListeningServer = server.serve(socket, factory)

      val transporter: (SocketAddress, StatsReceiver) => Future[Transport[Try[Int], Try[Int]]] =
        (_, _) => Future.value(clientTransport)

      val endpointer: (SocketAddress, StatsReceiver) => ServiceFactory[Try[Int], Try[Int]] =
        Bridge(transporter, (t: Transport[Try[Int], Try[Int]]) => new SerialClientDispatcher(t))

      val client: Client[Try[Int], Try[Int]] = DefaultClient("name", endpointer)
      val clientService: Service[Try[Int], Try[Int]] =
        client.newService(Name.fromGroup(listeningServer), "")

      val f = clientService(Return(4))
      val closed = listeningServer.close(1.second.fromNow)
      assert(closed.isDefined == false)
      p.setValue(Return(3))
      assert(f.isDefined == true)
      assert(closed.isDefined == true)
    }
  }
}

class FakeListener[T](qIn: AsyncQueue[T], qOut: AsyncQueue[T]) extends Listener[T, T] {
  override def listen(addr: SocketAddress)(serveTransport: Transport[T, T] => Unit): ListeningServer = {
    val transport = new QueueTransport(qIn, qOut)
    serveTransport(transport)
    NullServer
  }
}
