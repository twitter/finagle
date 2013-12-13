import com.twitter.finagle.dispatch.SerialServerDispatcher
import com.twitter.finagle.netty3.Netty3Listener
import com.twitter.finagle.server._
import com.twitter.finagle.Service
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Await, Future}

//#serverlistener
object EchoListener extends Netty3Listener[String, String](
  "echoListener", StringServerPipeline
)
//#serverlistener

object SimpleListenerExample {
  def main(args: Array[String]): Unit = {
    val address = new java.net.InetSocketAddress("localhost", 8080)
    //#simplelisten
    val service = new Service[String, String] {
      def apply(request: String) = Future.value(request)
    }
    val serveTransport = (t: Transport[String, String]) =>
      new SerialServerDispatcher(t, service)
    val server = EchoListener.listen(address) { serveTransport(_) }
    //#simplelisten

    Await.ready(Future.never)
  }
}

//#defaultserver
object EchoServer extends DefaultServer[String, String, String, String](
  "echoServer", EchoListener, new SerialServerDispatcher(_, _)
)
//#defaultserver

object EchoServerExample {
  def main(args: Array[String]): Unit = {
   //#defaultserverexample
    val service = new Service[String, String] {
      def apply(request: String) = Future.value(request)
    }
    Await.ready(EchoServer.serve(":8080", service))
   ////#defaultserverexample
  }
}