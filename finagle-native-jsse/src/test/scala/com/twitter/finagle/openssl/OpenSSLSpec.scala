import org.specs.Specification

import java.util.logging.Logger
import java.io.File

import org.jboss.netty.buffer._
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.http._

import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder, Http}
import com.twitter.finagle.service.Service
import com.twitter.util.{Future, RandomSocket}
import com.twitter.util.TimeConversions._

object SslConfig {
  val pwd = System.getenv("PWD")
  def here(filename: String): String =
    pwd + File.separator + filename

  // XXX - clean this shit up
  val serverCert = here("localhost.crt")
  val serverKey: String = here("localhost.key")
  val serverCiphers = "HIGH:MEDIUM:!aNULL:!eNULL:@STRENGTH:-DHE-RSA-AES128-SHA:-EDH-RSA-DES-CBC3-SHA:-DHE-RSA-AES256-SHA:-DHE-RSA-AES256-SHA"
}

object HarmonyJSSESpec extends Specification {
  "Harmony JSSE" should {
    "use the correct provider" in {
      val provider = new org.apache.harmony.xnet.provider.jsse.JSSEProvider
      provider.getName mustEqual "HarmonyJSSE"
    }

    "work" in {
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
        .service(service)
        .bindTo(address)
        .tls("test.keystore", "secret")
        .build

      val client =
        ClientBuilder()
        .name("http-client")
        .hosts(Seq(address))
        .codec(Http)
        .logger(Logger.getLogger("http"))
        .tlsWithoutValidation()
        .buildService[HttpRequest, HttpResponse]()

      val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
      val response = client(request)()
      response.getStatus mustEqual HttpResponseStatus.OK
      println("RESPONSE!!!")
      println(response)
    }
  }
}
