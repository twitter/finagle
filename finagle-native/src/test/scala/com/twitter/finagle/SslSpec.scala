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
    "works as a server" in {
      val address = RandomSocket.nextAddress

      val service = new Service[HttpRequest, HttpResponse] {
        def apply(request: HttpRequest) = Future {
          val response = new DefaultHttpResponse(
            HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
          response.setContent(ChannelBuffers.wrappedBuffer("yo".getBytes))
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

      val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
      val response = client(request)()
      response.getStatus mustEqual HttpResponseStatus.OK
    }
  }
}
