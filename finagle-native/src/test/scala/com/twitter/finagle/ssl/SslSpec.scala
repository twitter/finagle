package com.twitter.finagle.ssl

import com.twitter.finagle.Service
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.http.Http
import com.twitter.finagle.ssl.Ssl
import com.twitter.io.TempFile
import com.twitter.util.TimeConversions._
import com.twitter.util.Future
import java.io.File
import java.net.InetSocketAddress
import java.security.Provider
import org.jboss.netty.buffer._
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.http._
import org.specs.SpecificationWithJUnit

object Pwd {
  def pwd = System.getenv("PWD")

  def /(path: String): String =
    pwd + File.separator + path
}

object SslConfig {
  val certificateFile = TempFile.fromResourcePath(getClass, "/localhost.crt")
  val keyFile         = TempFile.fromResourcePath(getClass, "/localhost.key")

  val certificatePath = certificateFile.getAbsolutePath
  val keyPath: String = keyFile.getAbsolutePath
}

class SslSpec extends SpecificationWithJUnit {
  "automatically detected available provider" should {
    "be able to send and receive various sized content" in {
      def makeContent(length: Int) = {
        val buf = ChannelBuffers.directBuffer(length)
        while (buf.writableBytes() > 0)
          buf.writeByte('Z')
        buf
      }

      val service = new Service[HttpRequest, HttpResponse] {
        def apply(request: HttpRequest) = Future {
          val requestedBytes = request.getHeader("Requested-Bytes")
          match {
            case s: String => s.toInt
            case _ => 17280
          }
          val response = new DefaultHttpResponse(
            HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
          Option(request.getHeader("X-Transport-Cipher")) foreach { cipher: String =>
            response.setHeader("X-Transport-Cipher", cipher)
          }
          response.setContent(makeContent(requestedBytes))
          HttpHeaders.setContentLength(response, requestedBytes)

          response
        }
      }

      val codec =
        Http().annotateCipherHeader("X-Transport-Cipher")

      val server =
        ServerBuilder()
          .codec(codec)
          .bindTo(new InetSocketAddress(0))
          .tls(SslConfig.certificatePath, SslConfig.keyPath)
          .name("SSLServer")
          .build(service)

      def client =
        ClientBuilder()
          .name("http-client")
          .hosts(server.localAddress)
          .codec(codec)
          .hostConnectionLimit(1)
          .tlsWithoutValidation()
          .build()

      def test(requestSize: Int, responseSize: Int) {
        "%d byte request, %d byte response".format(requestSize, responseSize) in {
          val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")

          if (requestSize > 0) {
            request.setContent(makeContent(requestSize))
            HttpHeaders.setContentLength(request, requestSize)
          }

          if (responseSize > 0)
            request.setHeader("Requested-Bytes", responseSize)
          else
            request.setHeader("Requested-Bytes", 0)

          val response = client(request)()
          response.getStatus mustEqual HttpResponseStatus.OK
          HttpHeaders.getContentLength(response) mustEqual responseSize
          val content = response.getContent()

          content.readableBytes() mustEqual responseSize

          while (content.readableBytes() > 0) {
            assert(content.readByte() == 'Z')
          }

          val cipher = response.getHeader("X-Transport-Cipher")
          cipher must be_!=("null")
        }
      }

      test(   0 * 1024, 16   * 1024)
      test(  16 * 1024, 0    * 1024)
      test(1000 * 1024, 16   * 1024)
      test( 256 * 1024, 256  * 1024)
    }
  }
}
