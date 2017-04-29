package com.twitter.finagle.netty4.ssl.client

import com.twitter.finagle.{Address, Stack}
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.ssl.{KeyCredentials, TrustCredentials}
import com.twitter.finagle.ssl.client.{SslClientConfiguration, SslClientEngineFactory, SslContextClientEngineFactory}
import com.twitter.finagle.transport.Transport
import com.twitter.io.TempFile
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.ssl.SslHandler
import java.net.InetSocketAddress
import javax.net.ssl.SSLContext
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Netty4ClientSslHandlerTest extends FunSuite {

  def channel(ps: Stack.Params): EmbeddedChannel =
    new EmbeddedChannel(new Netty4ClientSslHandler(ps))

  def useKeyCredentials(): KeyCredentials = {
    val tempCertFile = TempFile.fromResourcePath("/ssl/certs/test-rsa.crt")
    // deleteOnExit is handled by TempFile

    val tempKeyFile = TempFile.fromResourcePath("/ssl/keys/test-pkcs8.key")
    // deleteOnExit is handled by TempFile

    KeyCredentials.CertAndKey(tempCertFile, tempKeyFile)
  }

  def withConfig(config: SslClientConfiguration): Stack.Params =
    Stack.Params.empty + Transport.ClientSsl(Some(config))

  def withContext(config: SslClientConfiguration): Stack.Params =
    withConfig(config) +
    SslClientEngineFactory.Param(new SslContextClientEngineFactory(SSLContext.getDefault))

  val paramsConfigurations: Seq[Stack.Params] = Seq(
    withConfig(SslClientConfiguration()),
    withConfig(SslClientConfiguration(hostname = Some("example.com"))),
    withConfig(SslClientConfiguration(keyCredentials = useKeyCredentials())),
    withConfig(SslClientConfiguration(trustCredentials = TrustCredentials.Insecure)),
    withContext(SslClientConfiguration()),
    withContext(SslClientConfiguration(hostname = Some("example.com"))))

  test("default doesn't contain ssl handlers") {
    val fakeAddr = InetSocketAddress.createUnresolved("foobar.com", 80)
    val ch = channel(Stack.Params.empty + Transporter.EndpointAddr(Address.Inet(fakeAddr, Map.empty)))
    val pipeline = ch.pipeline()

    // There is no Transport.ClientSsl param set, so these should be null
    val sslHandler = pipeline.get(classOf[SslHandler])
    assert(sslHandler == null)

    val sslConnectHandler = pipeline.get(classOf[SslClientConnectHandler])
    assert(sslConnectHandler == null)

    ch.finishAndReleaseAll()
  }

  test("auto-remove") {
    paramsConfigurations.foreach { params =>
      val ch = channel(params)
      val pipeline = ch.pipeline()

      val clientHandler = pipeline.get(classOf[Netty4ClientSslHandler])
      assert(clientHandler == null)

      ch.finishAndReleaseAll()
    }
  }

  test("client-side pipeline upgrade") {
    paramsConfigurations.foreach { params =>
      val fakeAddr = InetSocketAddress.createUnresolved("foobar.com", 80)
      val ch = channel(params + Transporter.EndpointAddr(Address.Inet(fakeAddr, Map.empty)))
      val pipeline = ch.pipeline()

      val sslHandler = pipeline.get(classOf[SslHandler])
      assert(sslHandler != null)

      val sslConnectHandler = pipeline.get(classOf[SslClientConnectHandler])
      assert(sslConnectHandler != null)

      val sslEngine = sslHandler.engine()
      assert(sslEngine.getUseClientMode)
      assert(Seq("example.com", "foobar.com").contains(sslEngine.getPeerHost))
      assert(sslEngine.getPeerPort == 80)

      ch.finishAndReleaseAll()
    }
  }

}
