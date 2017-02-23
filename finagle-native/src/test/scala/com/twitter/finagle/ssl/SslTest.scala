package com.twitter.finagle.ssl

import com.twitter.finagle.Service
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.http._
import com.twitter.finagle.ssl.server.{
  LegacyServerEngineFactory, SslServerConfiguration, SslServerEngineFactory}
import com.twitter.finagle.transport.Transport
import com.twitter.io.{Buf, TempFile}
import com.twitter.util.{Await, Future}
import java.net.{InetAddress, InetSocketAddress}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SslTest extends FunSuite {

  // now let's run some tests
  test("be able to send and receive various sized content") {
    def makeContent(length: Int): Buf =
      Buf.ByteArray.Owned(Array.fill(length)('Z'.toByte))

    val certFile = TempFile.fromResourcePath("/ssl/certs/svc-test-server.cert.pem")
    // deleteOnExit is handled by TempFile

    val keyFile = TempFile.fromResourcePath("/ssl/keys/svc-test-server-pkcs8.key.pem")
    // deleteOnExit is handled by TempFile

    val service = new Service[Request, Response] {
      def apply(request: Request) = Future {
        val requestedBytes = request.headerMap.get("Requested-Bytes") match {
          case Some(s) => s.toInt
          case None => 17280
        }
        val response = Response(Version.Http11, Status.Ok)
        request.headerMap.get("X-Transport-Cipher").foreach { cipher =>
          response.headerMap.set("X-Transport-Cipher", cipher)
        }
        response.content = makeContent(requestedBytes)
        response.contentLength = requestedBytes
        response
      }
    }

    val codec =
      Http().annotateCipherHeader("X-Transport-Cipher")

    val server =
      ServerBuilder()
        .codec(codec)
        .bindTo(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
        .tls(certFile.getAbsolutePath(), keyFile.getAbsolutePath())
        .name("SSLServer")
        .build(service)

    def client =
      ClientBuilder()
        .name("http-client")
        .hosts(server.boundAddress.asInstanceOf[InetSocketAddress])
        .codec(codec)
        .hostConnectionLimit(1)
        .tlsWithoutValidation()
        .build()

    def check(requestSize: Int, responseSize: Int) {
      val request = Request(Version.Http11, Method.Get, "/")

      if (requestSize > 0) {
        request.content = makeContent(requestSize)
        request.contentLength = requestSize
      }

      if (responseSize > 0)
        request.headerMap.set("Requested-Bytes", responseSize.toString)
      else
        request.headerMap.set("Requested-Bytes", 0.toString)

      val response = Await.result(client(request))
      assert(response.status == Status.Ok)
      assert(response.contentLength == Some(responseSize))
      val content = response.content

      assert(content.length == responseSize)

      assert(content == makeContent(responseSize))

      val cipher = response.headerMap.get("X-Transport-Cipher")
      assert(cipher != Some("null"))
    }

    check(   0 * 1024, 16   * 1024)
    check(  16 * 1024, 0    * 1024)
    check(1000 * 1024, 16   * 1024)
    check( 256 * 1024, 256  * 1024)
  }

  test("be able to validate a properly constructed authentication chain") {
    val certFile = TempFile.fromResourcePath("/ssl/certs/svc-test-server.cert.pem")
    // deleteOnExit is handled by TempFile

    val keyFile = TempFile.fromResourcePath("/ssl/keys/svc-test-server-pkcs8.key.pem")
    // deleteOnExit is handled by TempFile

    val chainFile = TempFile.fromResourcePath("/ssl/certs/svc-test-server-chain.cert.pem")
    // deleteOnExit is handled by TempFile

    val config = SslServerConfiguration(
      keyCredentials = KeyCredentials.CertKeyAndChain(certFile, keyFile, chainFile))

    val rootFile = TempFile.fromResourcePath("/ssl/certs/ca.cert.pem")
    // deleteOnExit is handled by TempFile

    // ... spin up an SSL server ...
    val service = new Service[Request, Response] {
      def apply(request: Request) = Future {
        def makeContent(length: Int): Buf =
          Buf.ByteArray.Owned(Array.fill(length)('Z'.toByte))

        val requestedBytes = request.headerMap.get("Requested-Bytes") match {
          case Some(s) => s.toInt
          case None => 17280
        }
        val response = Response(Version.Http11, Status.Ok)
        request.headerMap.get("X-Transport-Cipher").foreach { cipher =>
          response.headerMap.set("X-Transport-Cipher", cipher)
        }
        response.content = makeContent(requestedBytes)
        response.contentLength = requestedBytes

        response
      }
    }

    val codec =
      Http().annotateCipherHeader("X-Transport-Cipher")

    val server = ServerBuilder()
      .codec(codec)
      .bindTo(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
      .configured(Transport.ServerSsl(Some(config)))
      .configured(SslServerEngineFactory.Param(LegacyServerEngineFactory))
      .name("SSL server with valid certificate chain")
      .build(service)

    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]

    // ... then connect to that service using openssl and ensure that
    // the chain is correct
    val cmd = Array[String](
      "openssl", "s_client",
      "-connect",
      "localhost:" + addr.getPort.toString,
      "-CAfile",  rootFile.getAbsolutePath(),
      "-verify", "9", "-showcerts"
    )

    try {
      // would prefer to have an abstraction for what's below, but
      // Shell.run doesn't give you back the process
      val process = Runtime.getRuntime.exec(cmd)
      process.getOutputStream.write("QUIT\n".getBytes)
      process.getOutputStream.close()

      process.waitFor()
      assert(process.exitValue == 0)

      // look for text "Verify return code: 0 (ok)" on stdout
      val out = process.getInputStream
      val outBuf = new Array[Byte](out.available)
      out.read(outBuf)
      val outBufStr = new String(outBuf)
      assert("Verify return code: 0 \\(ok\\)".r.findFirstIn(outBufStr) == Some("""Verify return code: 0 (ok)"""))
    } catch {
      case ex: java.io.IOException =>
        println("Test skipped: running openssl failed" +
          " (openssl executable might be absent?)")
    }
  }
}
