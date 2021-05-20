package com.twitter.finagle.server

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.{Address, ClientConnection, Name, Service}
import com.twitter.finagle.client.utils.StringClient
import com.twitter.finagle.server.utils.StringServer
import com.twitter.finagle.ssl.{ClientAuth, KeyCredentials, TrustCredentials}
import com.twitter.finagle.ssl.client.SslClientConfiguration
import com.twitter.finagle.ssl.server.SslServerConfiguration
import com.twitter.finagle.ssl.session.NullSslSessionInfo
import com.twitter.io.TempFile
import com.twitter.util.registry.{Entry, GlobalRegistry, SimpleRegistry}
import com.twitter.util.{Await, Awaitable, Future, Promise}
import java.net.{InetAddress, InetSocketAddress, Socket}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import scala.util.control.NonFatal
import org.scalatest.funsuite.AnyFunSuite

class StringServerTest extends AnyFunSuite with Eventually with IntegrationPatience {

  private[this] val svc = Service.mk[String, String](Future.value)

  def await[T](t: Awaitable[T]): T = Await.result(t, 5.seconds)

  test("StringServer notices when the client cuts the connection") {
    val p = Promise[String]()
    @volatile var interrupted = false
    p.setInterruptHandler {
      case NonFatal(t) =>
        interrupted = true
    }
    @volatile var observedRequest: Option[String] = None

    val service = new Service[String, String] {
      def apply(request: String) = {
        observedRequest = Some(request)
        p
      }
    }

    val server =
      StringServer.server.serve(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), service)

    val client = new Socket()
    eventually { client.connect(server.boundAddress) }

    client.getOutputStream.write("hello netty4!\n".getBytes("UTF-8"))
    client.getOutputStream.flush()
    eventually { assert(observedRequest == Some("hello netty4!")) }

    client.close()
    eventually { assert(interrupted) }

    await(server.close())
  }

  test("exports listener type to registry") {
    val registry = new SimpleRegistry
    val label = "StringServer.server"

    val listeningServer = GlobalRegistry.withRegistry(registry) {
      StringServer.server
        .withLabel(label)
        .serve(":*", svc)
    }

    val expectedEntry = Entry(
      key = Seq("server", StringServer.protocolLibrary, label, "Listener"),
      value = "Netty4Listener"
    )

    assert(registry.iterator.contains(expectedEntry))

    await(listeningServer.close())
  }

  trait Ctx {
    val address = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val registry = ServerRegistry.connectionRegistry(address)

    val server = StringServer.server.serve(address, svc)
    val boundAddress = server.boundAddress.asInstanceOf[InetSocketAddress]

    val client1 = StringClient.client.newService(Name.bound(Address(boundAddress)), "stringClient1")
    val client2 = StringClient.client.newService(Name.bound(Address(boundAddress)), "stringClient2")

    registry.clear()
  }

  test("ConnectionRegistry has the right size") {
    new Ctx {
      val initialRegistrySize = registry.iterator.size

      assert(await(client1("hello")) == "hello")
      eventually {
        assert((registry.iterator.size - initialRegistrySize) == 1)
      }

      assert(await(client2("foo")) == "foo")
      eventually {
        assert((registry.iterator.size - initialRegistrySize) == 2)
      }

      await(client1.close())
      eventually {
        assert((registry.iterator.size - initialRegistrySize) == 1)
      }

      await(server.close())
      await(client2.close())
      eventually {
        assert((registry.iterator.size - initialRegistrySize) == 0)
      }
    }
  }

  test("ConnectionRegistry correctly removes entries upon client close") {
    new Ctx {
      val initialState: Array[ClientConnection] = registry.iterator.toArray

      assert(await(client1("hello")) == "hello")
      val clientConn1: ClientConnection = eventually {
        registry.iterator.find(!initialState.contains(_)).get
      }

      assert(await(client2("foo")) == "foo")
      val clientConn2: ClientConnection = eventually {
        registry.iterator.find(a => !initialState.contains(a) && a != clientConn1).get
      }

      await(client2.close())
      eventually {
        val connections = registry.iterator.toArray
        assert(connections.contains(clientConn1))
        assert(!(connections.contains(clientConn2)))
      }

      await(server.close())
      await(client1.close())
    }
  }

  trait SecureCtx {
    private val chainCert = TempFile.fromResourcePath("/ssl/certs/svc-test-chain.cert.pem")
    // deleteOnExit is handled by TempFile

    private val clientCert = TempFile.fromResourcePath("/ssl/certs/svc-test-client.cert.pem")
    // deleteOnExit is handled by TempFile

    private val clientKey = TempFile.fromResourcePath("/ssl/keys/svc-test-client-pkcs8.key.pem")
    // deleteOnExit is handled by TempFile

    private val serverCert = TempFile.fromResourcePath("/ssl/certs/svc-test-server.cert.pem")
    // deleteOnExit is handled by TempFile

    private val serverKey = TempFile.fromResourcePath("/ssl/keys/svc-test-server-pkcs8.key.pem")
    // deleteOnExit is handled by TempFile

    private val serverConfig = SslServerConfiguration(
      keyCredentials = KeyCredentials.CertAndKey(serverCert, serverKey),
      trustCredentials = TrustCredentials.CertCollection(chainCert),
      clientAuth = ClientAuth.Needed
    )

    private val clientConfig = SslClientConfiguration(
      keyCredentials = KeyCredentials.CertAndKey(clientCert, clientKey),
      trustCredentials = TrustCredentials.CertCollection(chainCert)
    )

    val address = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val registry = ServerRegistry.connectionRegistry(address)

    val server = StringServer.server.withTransport.tls(serverConfig).serve(address, svc)
    val boundAddress = server.boundAddress.asInstanceOf[InetSocketAddress]

    val client = StringClient.client.withTransport
      .tls(clientConfig)
      .newService(Name.bound(Address(boundAddress)), "stringClient")

    registry.clear()
  }

  test("ConnectionRegistry correctly provides SSL/TLS information for connections") {
    new SecureCtx {
      val initialState: Array[ClientConnection] = registry.iterator.toArray

      assert(await(client("hello")) == "hello")
      val clientConn: ClientConnection = eventually {
        registry.iterator.find(!initialState.contains(_)).get
      }

      assert(clientConn.sslSessionInfo != NullSslSessionInfo)

      await(server.close())
      await(client.close())
    }
  }
}
