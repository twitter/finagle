package com.twitter.finagle.server

import com.twitter.finagle._
import com.twitter.finagle.Stack.Params
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Await, Closable, Future, Time}
import java.net.{InetAddress, InetSocketAddress, SocketAddress}
import java.security.cert.{Certificate, X509Certificate}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mockito.MockitoSugar

@RunWith(classOf[JUnitRunner])
class StdStackServerTest extends FunSuite with MockitoSugar {

  val mockCert = mock[X509Certificate]
  private case class Server(
      stack: Stack[ServiceFactory[Unit, Unit]] = StackServer.newStack,
      params: Params = StackServer.defaultParams
    ) extends StdStackServer[Unit, Unit, Server] {

    override protected type In = Unit
    override protected type Out = Unit

    override protected def newListener(): Listener[In, Out] = new Listener[Unit, Unit] {
      override def listen(addr: SocketAddress)(serveTransport: (Transport[Unit, Unit]) => Unit): ListeningServer = {
        import org.mockito.Mockito.{when}
        val trans = mock[Transport[Unit, Unit]]
        when(trans.remoteAddress).thenReturn(mock[SocketAddress])
        when(trans.peerCertificate).thenReturn(Some(mockCert))
        when(trans.onClose).thenReturn(Future.never)
        serveTransport(trans)
        NullServer
      }
    }

    override protected def newDispatcher(transport: Transport[In, Out], service: Service[Unit, Unit]): Closable = Closable.nop

    override protected def copy1(s: Stack[ServiceFactory[Unit, Unit]], p: Params) = this.copy(s, p)
  }

  private class Factory extends ServiceFactory[Unit, Unit] {
    var cert: Option[Certificate] = None

    override def apply(conn: ClientConnection): Future[Service[Unit, Unit]] = {
      cert = Transport.peerCertificate
      Future.value(mock[Service[Unit, Unit]])
    }

    override def close(deadline: Time): Future[Unit] = Future.Done
  }

  test("peer certificate is available to service factory") {
    val factory = new Factory
    val server = new Server().serve( new InetSocketAddress(InetAddress.getLoopbackAddress, 0), factory )

    assert(factory.cert == Some(mockCert))
    Await.ready(server.close())
  }

}

