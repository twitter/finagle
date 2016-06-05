package com.twitter.finagle.netty4.ssl

import com.twitter.finagle.{Address, Stack}
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.transport.{Transport, TlsConfig}
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.ssl.SslHandler
import io.netty.handler.ssl.util.SelfSignedCertificate
import org.junit.runner.RunWith
import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{OneInstancePerTest, FunSuite}
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import java.net.InetSocketAddress
import javax.net.ssl.SSLContext

@RunWith(classOf[JUnitRunner])
class Netty4SslHandlerTest extends FunSuite
  with MockitoSugar
  with OneInstancePerTest
  with GeneratorDrivenPropertyChecks {

  val (certPath, keyPath) = {
    val ssc = new SelfSignedCertificate()
    (ssc.certificate().getPath, ssc.privateKey().getPath)
  }

  val genServerConfig: Gen[TlsConfig] = Gen.oneOf(
    TlsConfig.ServerSslContext(SSLContext.getDefault),
    TlsConfig.ServerCertAndKey(certPath, keyPath, None, None, None)
  )

  val genClientConfig: Gen[TlsConfig] = Gen.oneOf(
    TlsConfig.ClientHostname("example.com"),
    TlsConfig.ClientSslContext(SSLContext.getDefault),
    TlsConfig.ClientSslContextAndHostname(SSLContext.getDefault, "example.com"),
    TlsConfig.Client,
    TlsConfig.ClientNoValidation
  )

  val genConfigOrFail: Gen[TlsConfig] = Gen.oneOf(
    genServerConfig,
    genClientConfig,
    Gen.const(TlsConfig.ServerSslContext(null)),
    Gen.const(TlsConfig.ClientSslContext(null))
  )

  def channel(ps: Stack.Params): EmbeddedChannel =
    new EmbeddedChannel(new Netty4SslHandler(ps))

  test("auto-remove") {
    forAll(genConfigOrFail) { config =>
      val ch = channel(Stack.Params.empty + Transport.Tls(config))
      // The contract is that our handler is always removed.
      assert(ch.pipeline().get(classOf[Netty4SslHandler]) == null)

      ch.finishAndReleaseAll()
    }
  }

  test("server-side pipeline upgrade") {
    forAll(genServerConfig) { config =>
      val ch = channel(Stack.Params.empty + Transport.Tls(config))
      val ssl = ch.pipeline().get(classOf[SslHandler])

      assert(!ssl.engine().getUseClientMode)
      assert(ssl.engine().getEnableSessionCreation)

      ch.finishAndReleaseAll()
    }
  }

  test("client-side pipeline upgrade") {
    forAll(genClientConfig) { config =>
      val fakeAddr = InetSocketAddress.createUnresolved("foobar.com", 80)
      val ch = channel(Stack.Params.empty +
        Transport.Tls(config) +
        Transporter.EndpointAddr(Address.Inet(fakeAddr, Map.empty))
      )

      val ssl = ch.pipeline().get(classOf[SslHandler])

      assert(ch.pipeline().get(classOf[SslConnectHandler]) != null)
      assert(ssl.engine().getUseClientMode)
      assert(ssl.engine().getEnableSessionCreation)
      assert(ssl.engine().getPeerHost == "example.com" || ssl.engine().getPeerHost == "foobar.com")
      assert(ssl.engine().getPeerPort == 80)

      ch.finishAndReleaseAll()
    }
  }
}
