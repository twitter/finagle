package com.twitter.finagle.netty4

import com.twitter.conversions.time._
import com.twitter.finagle
import com.twitter.finagle.Stack.Params
import com.twitter.finagle._
import com.twitter.finagle.client.{StackClient, StdStackClient, Transporter}
import com.twitter.finagle.dispatch.{SerialClientDispatcher, SerialServerDispatcher}
import com.twitter.finagle.param.Label
import com.twitter.finagle.ssl.client.SslClientConfiguration
import com.twitter.finagle.ssl.server.SslServerConfiguration
import com.twitter.finagle.ssl.{ClientAuth, KeyCredentials, TrustCredentials}
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.transport.Transport.ServerSsl
import com.twitter.util.{Await, Future}
import io.netty.channel.ChannelPipeline
import io.netty.handler.codec.string.{StringDecoder, StringEncoder}
import io.netty.handler.codec.{DelimiterBasedFrameDecoder, Delimiters}
import io.netty.handler.ssl.util.SelfSignedCertificate
import java.net.{InetAddress, InetSocketAddress, SocketAddress}
import java.nio.charset.StandardCharsets.UTF_8

import org.scalatest.Outcome
import org.scalatest.fixture.FunSuite
import org.scalatest.concurrent.{Eventually, IntegrationPatience}

object Netty4SslTest {
  case class Client(
    clientCert: SelfSignedCertificate,
    serverCert: SelfSignedCertificate,
    params: Params = Params.empty,
    stack: Stack[ServiceFactory[String, String]] = StackClient.newStack
  ) extends StdStackClient[String, String, Client] {

    override protected type In = String
    override protected type Out = String

    override protected def newTransporter(addr: SocketAddress): Transporter[String, String] =
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
      transport: Transport[String, String]
    ): Service[String, String] =
      new SerialClientDispatcher(transport)

    override protected def copy1(stack: Stack[ServiceFactory[String, String]], params: Params) =
      copy(clientCert, serverCert, params, stack)
  }
}

class Netty4SslTest extends FunSuite with Eventually with IntegrationPatience {

  class Ctx {
    val serverCert = new SelfSignedCertificate("example.server.com")
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

    val server = {
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

      val serverConfig = SslServerConfiguration(
        keyCredentials =
          KeyCredentials.CertAndKey(serverCert.certificate(), serverCert.privateKey()),
        trustCredentials = TrustCredentials.CertCollection(clientCert.certificate()),
        clientAuth = ClientAuth.Needed
      )

      val p = Params.empty +
        ServerSsl(Some(serverConfig)) +
        Label("test")
      val listener = Netty4Listener[String, String](StringServerInit, p)
      val serveTransport = (t: Transport[String, String]) => {
        if (t.peerCertificate.isEmpty)
          throw new IllegalStateException("No peer certificate in transport")
        new SerialServerDispatcher(t, service)
      }
      listener.listen(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))(serveTransport(_))
    }

    val clientConfig = SslClientConfiguration(
      keyCredentials = KeyCredentials.CertAndKey(clientCert.certificate(), clientCert.privateKey()),
      trustCredentials = TrustCredentials.CertCollection(serverCert.certificate())
    )
    val client = {
      val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
      new finagle.netty4.Netty4SslTest.Client(clientCert, serverCert).withTransport
        .tls(clientConfig)
        .newService(s"${addr.getHostName}:${addr.getPort}", "client")
    }

    def close() = Future.collect(Seq(client.close(), server.close()))
  }

  override type FixtureParam = Ctx

  override def withFixture(test: OneArgTest): Outcome = {
    val ctx = new Ctx
    try {
      withFixture(test.toNoArgTest(ctx))
    } finally {
      Await.ready(ctx.close(), 3.seconds)
    }
  }

  test("Peer certificate is available to service") { ctx =>
    val response = Await.result(ctx.client("security is overrated!\n"), 3.seconds)
    assert(response == "OK")
  }
}
