//#imports
import com.twitter.finagle.{Http, Service}
import com.twitter.finagle.http
import com.twitter.util.{Await, Future}
//#imports

object Server extends App {
//#service
  val service = new Service[http.Request, http.Response] {
    def apply(req: http.Request): Future[http.Response] =
      Future.value(
        http.Response(req.version, http.Status.Ok)
      )
  }
//#service
//#builder
  val server = Http.serve(":8080", service)
  Await.ready(server)
//#builder
}
