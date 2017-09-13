package com.twitter.finagle.netty4

import com.twitter.finagle
import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.Stack.Params
import com.twitter.finagle.client.{StackClient, StdStackClient, Transporter}
import com.twitter.finagle.dispatch.{SerialClientDispatcher, SerialServerDispatcher}
import com.twitter.finagle.netty4.ssl.server.SniSupport
import com.twitter.finagle.param.Label
import com.twitter.finagle.ssl.{ClientAuth, Engine, KeyCredentials, TrustCredentials}
import com.twitter.finagle.ssl.client.{SslClientConfiguration, SslClientEngineFactory}
import com.twitter.finagle.ssl.server.{SslServerConfiguration, SslServerEngineFactory}
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.finagle.transport.Transport.ServerSsl
import com.twitter.util.{Await, Future, Throw}
import io.netty.channel.ChannelPipeline
import io.netty.handler.codec.{DelimiterBasedFrameDecoder, Delimiters}
import io.netty.handler.codec.string.{StringDecoder, StringEncoder}
import io.netty.handler.ssl.{SslContext, SslContextBuilder}
import io.netty.handler.ssl.util.SelfSignedCertificate
import io.netty.util.concurrent.{Future => NettyFuture}
import java.net.{InetAddress, InetSocketAddress, SocketAddress}
import java.nio.charset.StandardCharsets.UTF_8
import java.security.cert.X509Certificate
import javax.net.ssl.SSLException

import org.scalatest.{Assertion, Outcome}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.fixture.FunSuite

object Netty4SslTest {
  case class Client(
      params: Params = Params.empty,
      stack: Stack[ServiceFactory[String, String]] = StackClient.newStack
   ) extends StdStackClient[String, String, Client] {

    protected type In = String
    protected type Out = String
    protected type Context = TransportContext

    override protected def newTransporter(
      addr: SocketAddress
    ): Transporter[String, String, TransportContext] =
      Netty4Transporter.raw[String, String](
        pipeline => {
          pipeline.addLast(new DelimiterBasedFrameDecoder(8192, Delimiters.lineDelimiter(): _*))
          pipeline.addLast(new StringDecoder())
          pipeline.addLast(new StringEncoder())
        },
        addr,
        params
      )

    override protected def newDispatcher(
      transport: Transport[String, String] { type Context <: Client.this.Context }
    ): Service[String, String] =
      new SerialClientDispatcher(transport)

    override protected def copy1(stack: Stack[ServiceFactory[String, String]], params: Params) =
      copy(params, stack)
  }
  abstract class Ctx {
    val clientCert = new SelfSignedCertificate("example.client.com")
    val allocator = io.netty.buffer.UnpooledByteBufAllocator.DEFAULT

    private object StringServerInit extends (ChannelPipeline => Unit) {
      def apply(pipeline: ChannelPipeline): Unit = {
        pipeline.addLast(
          "line",
          new DelimiterBasedFrameDecoder(100, Delimiters.lineDelimiter(): _*)
        )
        pipeline.addLast("stringDecoder", new StringDecoder(UTF_8))
        pipeline.addLast("stringEncoder", new StringEncoder(UTF_8))
      }
    }

    protected def mkServer(sslParams: Params): ListeningServer = {
      val service =
        new Service[String, String] {
          override def apply(request: String): Future[String] = {
            Future.value(
              Transport.peerCertificate match {
                case Some(_) => "OK\n"
                case None => "ERROR\n"
              }
            )
          }
        }


      val p = Params.empty +
        ServerSsl(Some(SslServerConfiguration(clientAuth = ClientAuth.Needed))) +
        Label("test") ++ sslParams
      val listener = Netty4Listener[String, String](StringServerInit, p)
      val serveTransport = (t: Transport[String, String]) => {
        if (t.peerCertificate.isEmpty)
          throw new IllegalStateException("No peer certificate in transport")
        new SerialServerDispatcher(t, service)
      }
      listener.listen(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))(serveTransport(_))
    }

    def server: ListeningServer

    def mkClient(serverCert: X509Certificate, hostname: String): Service[String, String] = {
      val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
      new finagle.netty4.Netty4SslTest.Client().withTransport
        .tls(SslClientConfiguration(Some(hostname)), new SslClientEngineFactory {
        override def apply(address: Address, config: SslClientConfiguration): Engine = {
          val ctx = SslContextBuilder.forClient.keyManager(clientCert.key(), clientCert.cert()).trustManager(serverCert).build()
          new Engine(ctx.newEngine(allocator, hostname, -1))
        }
      }).newService(s"${addr.getHostName}:${addr.getPort}", "client")
    }

    def client: Service[String, String]

    def close() = Future.collect(Seq(client.close(), server.close()))
  }

  class DefaultCtx extends Ctx {
    val serverCert = new SelfSignedCertificate("example.server.com")
    val serverConfig = SslServerConfiguration(
      keyCredentials = KeyCredentials.CertAndKey(serverCert.certificate(), serverCert.privateKey()),
      trustCredentials = TrustCredentials.CertCollection(clientCert.certificate()),
      clientAuth = ClientAuth.Needed)

    override def server: ListeningServer = mkServer(Params.empty + ServerSsl(Some(serverConfig)))

    override def client: Service[String, String] = mkClient(serverCert.cert(), "example.server.com")
  }

  class SniCtx extends Ctx {

    val serverName = "first.server.com"
    val serverCerts = Map(
      serverName -> new SelfSignedCertificate(serverName),
      "second.server.com" -> new SelfSignedCertificate("second.server.com")
    )

    private def mkConfig(serverCert: SelfSignedCertificate): SslServerConfiguration = {
      SslServerConfiguration(
        keyCredentials = KeyCredentials.CertAndKey(serverCert.certificate(), serverCert.privateKey()),
        trustCredentials = TrustCredentials.CertCollection(clientCert.certificate()),
        clientAuth = ClientAuth.Needed
      )
    }

    override def server: ListeningServer =
      mkServer(Params.empty + ServerSsl(Some(SslServerConfiguration(clientAuth = ClientAuth.Needed))) +
        SniSupport.fromOption(serverCerts.get(_).map(mkConfig)))

    override def client: Service[String, String] = {
      val (hostname, serverCrt) = serverCerts.head
      mkClient(serverCrt.cert(), hostname)
    }
  }
}

abstract class Netty4SslTest[A <: Netty4SslTest.Ctx] extends FunSuite with Eventually with IntegrationPatience {

  def mkContext: A

  override type FixtureParam = A

  override def withFixture(test: OneArgTest): Outcome = {
    val ctx = mkContext
    try {
      withFixture(test.toNoArgTest(ctx))
    } finally {
      Await.ready(ctx.close(), 3.seconds)
    }
  }

}

class Netty4NoSniSslTest extends Netty4SslTest[Netty4SslTest.DefaultCtx] {

  test("Peer certificate is available to service") { ctx =>
    val response = Await.result(ctx.client("security is overrated!\n"), 3.seconds)
    assert(response == "OK")
  }

  override def mkContext: Netty4SslTest.DefaultCtx = new Netty4SslTest.DefaultCtx
}

class Netty4SniSslTest extends Netty4SslTest[Netty4SslTest.SniCtx] {
  override val mkContext: Netty4SslTest.SniCtx = new Netty4SslTest.SniCtx

  test("server uses certificate assigned to the server name sent by client") { ctx =>
    val response = Await.result(ctx.client("security is overrated!\n"), 3.seconds)
    assert(response == "OK")
  }

  test("Unknown hostname") { ctx =>
    val serverCert = new SelfSignedCertificate("unknown.server.com")
    val client = ctx.mkClient(serverCert.cert(), "unknown.server.com")
    try {
      val future: Future[Assertion] = client("let it crash").transform {
        case Throw(Failure(Some(t: ConnectionFailedException))) if t.getCause.isInstanceOf[SSLException] =>
          Future.value(succeed)
        case x =>
          Future.value(fail("Ssl handshake should fail."))
      }
        Await.result(future, 3.seconds)
    } finally {
      Await.ready(client.close(), 3.second)
    }
  }
}
