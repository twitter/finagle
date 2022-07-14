package com.twitter.finagle.server

import com.twitter.finagle.Stack.Params
import com.twitter.finagle._
import com.twitter.finagle.ssl.session.SslSessionInfo
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.transport.TransportContext
import com.twitter.util.Await
import com.twitter.util.Closable
import com.twitter.util.Future
import com.twitter.util.Time
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.SocketAddress
import java.security.cert.Certificate
import java.security.cert.X509Certificate
import org.mockito.Mockito
import org.mockito.stubbing.OngoingStubbing
import org.scalatestplus.mockito.MockitoSugar
import scala.language.reflectiveCalls
import org.scalatest.funsuite.AnyFunSuite

class StdStackServerTest extends AnyFunSuite with MockitoSugar {

  // Don't let the Scala compiler get confused about which `thenReturn`
  // method we want to use.
  private[this] def when[T](o: T) =
    Mockito.when(o).asInstanceOf[{ def thenReturn[RT](s: RT): OngoingStubbing[RT] }]

  val mockCert = mock[X509Certificate]
  private case class Server(
    stack: Stack[ServiceFactory[Unit, Unit]] = StackServer.newStack,
    params: Params = StackServer.defaultParams)
      extends StdStackServer[Unit, Unit, Server] {

    protected type In = Unit
    protected type Out = Unit
    protected type Context = TransportContext

    override protected def newListener(): Listener[In, Out, TransportContext] =
      new Listener[Unit, Unit, TransportContext] {
        override def listen(
          addr: SocketAddress
        )(
          serveTransport: (Transport[Unit, Unit] { type Context <: Server.this.Context }) => Unit
        ): ListeningServer = {
          val trans = mock[Transport[Unit, Unit]]
          val context = mock[TransportContext]
          val sslSessionInfo = mock[SslSessionInfo]
          when(sslSessionInfo.peerCertificates).thenReturn(Seq(mockCert))
          when(context.sslSessionInfo).thenReturn(sslSessionInfo)
          when(trans.context).thenReturn(context)
          when(trans.context.remoteAddress).thenReturn(mock[SocketAddress])
          when(trans.onClose).thenReturn(Future.never)
          serveTransport(trans)
          NullServer
        }
      }

    override protected def newDispatcher(
      transport: Transport[In, Out] { type Context <: Server.this.Context },
      service: Service[Unit, Unit]
    ): Closable = Closable.nop

    override protected def copy1(s: Stack[ServiceFactory[Unit, Unit]], p: Params) = this.copy(s, p)
  }

  private class Factory extends ServiceFactory[Unit, Unit] {
    var cert: Option[Certificate] = None

    override def apply(conn: ClientConnection): Future[Service[Unit, Unit]] = {
      cert = Transport.peerCertificate
      Future.value(mock[Service[Unit, Unit]])
    }

    override def close(deadline: Time): Future[Unit] = Future.Done
    def status: Status = Status.Open
  }

  test("peer certificate is available to service factory") {
    val factory = new Factory
    val server =
      new Server().serve(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), factory)

    assert(factory.cert == Some(mockCert))
    Await.ready(server.close())
  }

}
