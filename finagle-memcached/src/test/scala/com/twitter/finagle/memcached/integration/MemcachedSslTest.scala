package com.twitter.finagle.memcached.integration

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Memcached.Server
import com.twitter.finagle.client.StackClient
import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.memcached.integration.MemcachedSslTest.{
  chainCert,
  clientCert,
  clientKey,
  serverCert,
  serverKey
}
import com.twitter.finagle.memcached.integration.external.InProcessMemcached
import com.twitter.finagle.memcached.partitioning.MemcachedPartitioningService
import com.twitter.finagle.memcached.protocol.{Command, Response}
import com.twitter.finagle.memcached.util.AtomicMap
import com.twitter.finagle.memcached.{Interpreter, InterpreterService, TwemcacheClient}
import com.twitter.finagle.naming.BindingFactory
import com.twitter.finagle.partitioning.param
import com.twitter.finagle.service.TimeoutFilter
import com.twitter.finagle.ssl.client.SslClientConfiguration
import com.twitter.finagle.ssl.server.SslServerConfiguration
import com.twitter.finagle.ssl.{KeyCredentials, TrustCredentials}
import com.twitter.finagle.{Address, ListeningServer, Memcached, Name, ServiceFactory, Stack}
import com.twitter.hashing.KeyHasher
import com.twitter.io.{Buf, TempFile}
import com.twitter.util.{Await, Awaitable}
import java.net.{InetAddress, InetSocketAddress}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

object MemcachedSslTest {

  private val chainCert = TempFile.fromResourcePath("/ssl/certs/svc-test-chain.cert.pem")
  // deleteOnExit is handled by TempFile

  private val serverCert = TempFile.fromResourcePath("/ssl/certs/svc-test-server.cert.pem")
  // deleteOnExit is handled by TempFile

  private val serverKey = TempFile.fromResourcePath("/ssl/keys/svc-test-server-pkcs8.key.pem")
  // deleteOnExit is handled by TempFile

  private val clientCert = TempFile.fromResourcePath("/ssl/certs/svc-test-client.cert.pem")
  // deleteOnExit is handled by TempFile

  private val clientKey = TempFile.fromResourcePath("/ssl/keys/svc-test-client-pkcs8.key.pem")
  // deleteOnExit is handled by TempFile
}

class MemcachedSslTest extends AnyFunSuite with BeforeAndAfterAll {

  protected[this] def await[T](awaitable: Awaitable[T]): T = Await.result(awaitable, 15.seconds)

  class SslMemcachedServer(serverConfig: SslServerConfiguration) {

    private[this] val service: InterpreterService = {
      val interpreter = new Interpreter(new AtomicMap(InProcessMemcached.initialMaps))
      new InterpreterService(interpreter)
    }

    private[this] val serverSpec: Server =
      Memcached.server.withTransport.tls(serverConfig).withLabel("finagle")

    private[this] var server: Option[ListeningServer] = None

    def start(): ListeningServer = {
      val address = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
      server = Some(serverSpec.serve(address, service))
      server.get
    }

    def stop(blocking: Boolean = false): Unit = {
      server.foreach { server =>
        if (blocking) Await.result(server.close())
        else server.close()
        this.server = None
      }
    }
  }

  private[this] def newClientStack(): Stack[ServiceFactory[Command, Response]] = {
    // create a partitioning aware finagle client by inserting the PartitioningService appropriately
    StackClient
      .newStack[Command, Response]
      .insertAfter(
        BindingFactory.role,
        MemcachedPartitioningService.module
      )
  }

  def createClient(dest: Name, label: String, clientConfig: SslClientConfiguration) = {
    TwemcacheClient(
      Memcached.client
        .configured(param.KeyHasher(KeyHasher.KETAMA))
        .configured(TimeoutFilter.Param(10000.milliseconds))
        .configured(param.EjectFailedHost(false))
        .configured(LoadBalancerFactory.ReplicateAddresses(2))
        .withStack(newClientStack())
        .withTransport.tls(clientConfig)
        .newService(dest, label)
    )
  }
  private val serverConfig1 = SslServerConfiguration(
    keyCredentials = KeyCredentials.CertAndKey(serverCert, serverKey),
    trustCredentials = TrustCredentials.CertCollection(chainCert)
  )

  val server1 = new SslMemcachedServer(serverConfig1)

  private val serverConfig2 = SslServerConfiguration(
    keyCredentials = KeyCredentials.CertKeyAndChain(serverCert, serverKey, chainCert)
  )

  val server2 = new SslMemcachedServer(serverConfig2)

  override def afterAll(): Unit = {
    server1.stop()
    server2.stop()
  }

  test("server configured with 3rdparty credentials") {
    val clientConfig = SslClientConfiguration(
      keyCredentials = KeyCredentials.CertsAndKey(clientCert, clientKey),
      trustCredentials = TrustCredentials.CertCollection(chainCert)
    )

    val client = createClient(
      Name.bound(Address(server1.start().boundAddress.asInstanceOf[InetSocketAddress])),
      "test_ssl_client",
      clientConfig)
    await(client.set("foo", Buf.Utf8("bar")))
    assert(await(client.get("foo")).get == Buf.Utf8("bar"))
    await(client.close())
  }

  test("server configured with self provided credentials") {
    val clientConfig = SslClientConfiguration(
      keyCredentials = KeyCredentials.CertsAndKey(clientCert, clientKey),
      trustCredentials = TrustCredentials.CertCollection(chainCert)
    )

    val client = createClient(
      Name.bound(Address(server2.start().boundAddress.asInstanceOf[InetSocketAddress])),
      "test_ssl_client",
      clientConfig)
    await(client.set("foo", Buf.Utf8("bar")))
    assert(await(client.get("foo")).get == Buf.Utf8("bar"))
    await(client.close())
  }

}
