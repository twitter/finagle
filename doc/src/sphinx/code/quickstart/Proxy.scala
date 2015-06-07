import com.twitter.finagle.{Httpx, Service}
import com.twitter.finagle.httpx.{Request, Response}
import com.twitter.util.Await

//#app
object Proxy extends App {
  val client: Service[Request, Response] =
    Httpx.newService("www.google.com:80")

  val server = Httpx.serve(":8080", client)
  Await.ready(server)
}
//#app
