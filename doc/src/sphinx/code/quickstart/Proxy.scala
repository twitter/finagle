import com.twitter.finagle.{Http, Service}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.util.Await

//#app
object Proxy extends App {
  val client: Service[Request, Response] =
    Http.newService("www.google.com:80")

  val server = Http.serve(":8080", client)
  Await.ready(server)
}
//#app
