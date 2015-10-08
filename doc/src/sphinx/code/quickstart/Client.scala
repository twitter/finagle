import com.twitter.finagle.{Http, Service}
import com.twitter.finagle.http
import com.twitter.util.{Await, Future}

object Client extends App {
//#builder
  val client: Service[http.Request, http.Response] = Http.newService("www.scala-lang.org:80")
//#builder
//#dispatch
  val request = http.Request(http.Method.Get, "/")
  request.host = "www.scala-lang.org"
  val response: Future[http.Response] = client(request)
//#dispatch
//#callback
  response.onSuccess { resp: http.Response =>
    println("GET success: " + resp)
  }
  Await.ready(response)
//#callback
}
