package com.twitter.finagle.netty4.ssl.server

import com.twitter.finagle.Stack
import com.twitter.finagle.ssl.KeyCredentials
import com.twitter.finagle.ssl.server.{
  SslServerConfiguration,
  SslServerEngineFactory,
  SslContextServerEngineFactory
}
import com.twitter.finagle.transport.Transport
import com.twitter.io.TempFile
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.ssl.SslHandler
import javax.net.ssl.SSLContext
import org.scalatest.funsuite.AnyFunSuite

class Netty4ServerSslHandlerTest extends AnyFunSuite {

  def channel(ps: Stack.Params): EmbeddedChannel =
    new EmbeddedChannel(new Netty4ServerSslChannelInitializer(ps))

  def useKeyCredentials(): KeyCredentials = {
    val tempCertFile = TempFile.fromResourcePath("/ssl/certs/svc-test-server.cert.pem")
    // deleteOnExit is handled by TempFile

    val tempKeyFile = TempFile.fromResourcePath("/ssl/keys/svc-test-server-pkcs8.key.pem")
    // deleteOnExit is handled by TempFile

    KeyCredentials.CertAndKey(tempCertFile, tempKeyFile)
  }

  def withConfig(config: SslServerConfiguration): Stack.Params =
    Stack.Params.empty + Transport.ServerSsl(Some(config))

  def withContext(config: SslServerConfiguration): Stack.Params =
    withConfig(config) +
      SslServerEngineFactory.Param(new SslContextServerEngineFactory(SSLContext.getDefault))

  val paramsConfiguration: Seq[Stack.Params] = Seq(
    withConfig(SslServerConfiguration(keyCredentials = useKeyCredentials())),
    withContext(SslServerConfiguration())
  )

  test("default doesn't contain ssl handler") {
    val ch = channel(Stack.Params.empty)
    val pipeline = ch.pipeline()

    // There is no Tranport.ServerSsl param set, so this should be null
    val sslHandler = pipeline.get(classOf[SslHandler])
    assert(sslHandler == null)

    ch.finishAndReleaseAll()
  }

  test("auto-remove") {
    paramsConfiguration.foreach { params =>
      val ch = channel(params)
      val pipeline = ch.pipeline()

      val channelInitializer = pipeline.get(classOf[Netty4ServerSslChannelInitializer])
      assert(channelInitializer == null)

      ch.finishAndReleaseAll()
    }
  }

  test("server-side pipeline upgrade") {
    paramsConfiguration.foreach { params =>
      val ch = channel(params)
      val pipeline = ch.pipeline()

      val sslHandler = pipeline.get(classOf[SslHandler])
      assert(sslHandler != null)

      val sslEngine = sslHandler.engine()
      assert(!sslEngine.getUseClientMode)

      ch.finishAndReleaseAll()
    }
  }

}
