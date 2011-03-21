import org.specs.Specification

import java.io.File
import java.security.Provider

import org.jboss.netty.buffer._
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.http._

import com.twitter.finagle.Service
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder, Http, Ssl}
import com.twitter.finagle.builder.Ssl.ContextFactory

import com.twitter.util.{Future, RandomSocket}
import com.twitter.util.TimeConversions._

object Pwd {
  def pwd = System.getenv("PWD")

  def /(path: String): String =
    pwd + File.separator + path
}

object SslConfig {
  val certificatePath = Pwd / "finagle-native/src/test/resources/localhost.crt"
  val keyPath: String = Pwd / "finagle-native/src/test/resources/localhost.key"
}

object SslSpec extends Specification {
  def isNativeProvider(provider: Provider) =
    provider.toString.startsWith("HarmonyJSSE")
  def isDefaultProvider(provider: Provider) =
    provider.toString.startsWith("SunJSSE")

  def testContextFactory(factory: ContextFactory) {
    val ctx = factory.context(SslConfig.certificatePath, SslConfig.keyPath)

    ctx mustNot beNull

    if (factory == Ssl.DefaultJSSEContextFactory)
      isDefaultProvider(ctx.getProvider) must beTrue
    else
      isNativeProvider(ctx.getProvider) must beTrue
  }

  "Default SSL provider" should {
    "be backed by the Sun JSSE Provider and able to be instantiated" in {
      testContextFactory(Ssl.DefaultJSSEContextFactory)
    }
  }

  "Native SSL provider" should {
    "be backed by the Harmony JSSE and able to be instantiated" in {
      if (!Ssl.isNativeProviderAvailable())
        skip("Native provider is not available.")
      testContextFactory(Ssl.NativeJSSEContextFactory)
    }
  }

  "automatically detected available provider" should {
    "be able to send and receive various sized content" in {
      val address = RandomSocket.nextAddress

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
          response.setContent(makeContent(requestedBytes))
          response
        }
      }

      val server =
        ServerBuilder()
          .codec(Http)
          .bindTo(address)
          .tls(SslConfig.certificatePath, SslConfig.keyPath)
          .build(service)

      val client =
        ClientBuilder()
          .name("http-client")
          .hosts(Seq(address))
          .codec(Http)
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
        }
      }

      test(   0 * 1024, 16   * 1024)
      test(  16 * 1024, 0    * 1024)
      test(1000 * 1024, 16   * 1024)
      test( 256 * 1024, 256  * 1024)
      test(  16 * 1024, 2000 * 1024)
    }
  }
}
