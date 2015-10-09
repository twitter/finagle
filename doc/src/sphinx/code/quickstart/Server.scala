//#imports
import com.twitter.finagle.{Httpx, Service}
import com.twitter.finagle.httpx
import com.twitter.util.{Await, Future}
//#imports

object Server extends App {
//#service
  val service = new Service[httpx.Request, httpx.Response] {
    def apply(req: httpx.Request): Future[httpx.Response] =
      Future.value(
        httpx.Response(req.version, httpx.Status.Ok)
      )
  }
//#service
//#builder
  val server = Httpx.serve(":8080", service)
  Await.ready(server)
//#builder
}
