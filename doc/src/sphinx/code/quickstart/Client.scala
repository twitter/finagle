import com.twitter.finagle.{Http, Service}
import com.twitter.util.{Await, Future}
import java.net.InetSocketAddress
import org.jboss.netty.handler.codec.http._

object Client extends App {
//#builder
  val client: Service[HttpRequest, HttpResponse] =
    Http.newService("www.google.com:80")
//#builder
//#dispatch
  val request =  new DefaultHttpRequest(
    HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
  val response: Future[HttpResponse] = client(request)
//#dispatch
//#callback
  response onSuccess { resp: HttpResponse =>
    println("GET success: " + resp)
  }
  Await.ready(response)
//#callback
}
