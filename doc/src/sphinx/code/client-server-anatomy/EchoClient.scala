import com.twitter.conversions.time._
import com.twitter.finagle.client._
import com.twitter.finagle.dispatch.SerialClientDispatcher
import com.twitter.finagle.filter.MaskCancelFilter
import com.twitter.finagle.netty3.Netty3Transporter
import com.twitter.finagle.service.{RetryingFilter, RetryPolicy, TimeoutFilter}
import com.twitter.finagle.stats.{NullStatsReceiver, InMemoryStatsReceiver}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.{Group, Service, ServiceProxy, SimpleFilter}
import com.twitter.util.{Await, Future, Try, Throw, Time}

//#transporter
object StringClientTransporter extends Netty3Transporter[String, String](
  name = "StringClientTransporter",
  pipelineFactory = StringClientPipeline)
//#transporter

object BasicClient {
  //#explicitbridge
  val sa = new java.net.InetSocketAddress("localhost", 8080)
  val sr = com.twitter.finagle.stats.NullStatsReceiver

  val bridge: Future[Service[String, String]] =
    StringClientTransporter(sa, sr) map { transport =>
      new SerialClientDispatcher(transport)
    }

  val client = new Service[String, String] {
    def apply(req: String) = bridge flatMap { svc =>
      svc(req) ensure svc.close()
    }
  }
  //#explicitbridge
}

object BasicClientExample extends App {
  import BasicClient._

  //#basicclientexample
  val result = client("hello")
  println(Await.result(result))
  //#basicclientexample
}

object Filters {
  //#filters
  val retry = new RetryingFilter[String, String](
    retryPolicy = RetryPolicy.tries(3),
    timer = DefaultTimer.twitter
  )

  val timeout = new TimeoutFilter[String, String](
    timeout = 3.seconds,
    timer = DefaultTimer.twitter
  )

  val maskCancel = new MaskCancelFilter[String, String]
  //#filters
}

object RobustClientExample extends App {
  import BasicClient._
  import Filters._

  //#robustclient
  val newClient =
    retry andThen
    timeout andThen
    maskCancel andThen client

  val result = newClient("hello")
  println(Await.result(result))
  //#robustclient
}

object DefaultClientExample extends App {
  import Filters._
  //#defaultclient
  object EchoClient extends DefaultClient[String, String](
    name = "EchoClient",
    endpointer = {
      val bridge = Bridge[String, String, String, String](
        StringClientTransporter, new SerialClientDispatcher(_)
      )
      (addr, stats) => bridge(addr, stats)
    }
  )

  val client: Service[String, String] =
    retry andThen
    timeout andThen EchoClient.newService("localhost:8080")
  //#defaultclient

  val result = client("hello")
  println(Await.result(result))
}

