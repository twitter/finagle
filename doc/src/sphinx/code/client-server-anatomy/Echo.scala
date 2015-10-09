import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.client.{StackClient, Transporter, StdStackClient}
import com.twitter.finagle.dispatch.{SerialClientDispatcher, SerialServerDispatcher}
import com.twitter.finagle.filter.MaskCancelFilter
import com.twitter.finagle.netty3.{Netty3Transporter, Netty3Listener}
import com.twitter.finagle.server.{StackServer, Listener, StdStackServer}
import com.twitter.finagle.service.{RetryExceptionsFilter, RetryPolicy, TimeoutFilter}
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{Future, Await}
import java.net.SocketAddress

object Echo extends Client[String, String] with Server[String, String] {
  //#client
  case class Client(
    stack: Stack[ServiceFactory[String, String]] = StackClient.newStack,
    params: Stack.Params = StackClient.defaultParams
  ) extends StdStackClient[String, String, Client] {
    protected type In = String
    protected type Out = String

    protected def copy1(
        stack: Stack[ServiceFactory[String, String]],
        params: Stack.Params): Client =
      copy(stack, params)

    //#transporter
    protected def newTransporter(): Transporter[String, String] =
      Netty3Transporter(StringClientPipeline, params)
    //#transporter

    protected def newDispatcher(
        transport: Transport[String, String]): Service[String, String] =
      new SerialClientDispatcher(transport)
  }
  //#client

  val client = Client()

  def newService(dest: Name, label: String): Service[String, String] =
    client.newService(dest, label)

  def newClient(dest: Name, label: String): ServiceFactory[String, String] =
    client.newClient(dest, label)


  //#server
  case class Server(
    stack: Stack[ServiceFactory[String, String]] = StackServer.newStack,
    params: Stack.Params = StackServer.defaultParams
  ) extends StdStackServer[String, String, Server] {
    protected type In = String
    protected type Out = String

    protected def copy1(
      stack: Stack[ServiceFactory[String, String]] = this.stack,
      params: Stack.Params = this.params
    ): Server = copy(stack, params)

    //#serverlistener
    protected def newListener(): Listener[String, String] =
      Netty3Listener(StringServerPipeline, params)
    //#serverlistener

    protected def newDispatcher(
        transport: Transport[String, String],
        service: Service[String, String]) =
      new SerialServerDispatcher(transport, service)
  }
  //#server

  val server = Server()

  def serve(addr: SocketAddress,
      service: ServiceFactory[String, String]): ListeningServer =
    server.serve(addr, service)
}


object SimpleListenerExample {
  def main(args: Array[String]): Unit = {
    val address = new java.net.InetSocketAddress("localhost", 8080)
    //#simplelisten
    val service = new Service[String, String] {
      def apply(request: String) = Future.value(request)
    }
    val serveTransport = (t: Transport[String, String]) =>
      new SerialServerDispatcher(t, service)
    val listener = Netty3Listener[String, String](
      StringServerPipeline, StackServer.defaultParams)
    val server = listener.listen(address) { serveTransport(_) }
    //#simplelisten

    Await.ready(Future.never)
  }
}

object EchoServerExample {
  def main(args: Array[String]): Unit = {
    //#serveruse
    val service = new Service[String, String] {
      def apply(request: String) = Future.value(request)
    }
    val server = Echo.serve(":8080", service)
    Await.result(server)
    //#serveruse
  }
}

object BasicClient {
  //#explicitbridge
  val addr = new java.net.InetSocketAddress("localhost", 8080)
  val transporter = Netty3Transporter[String, String](
    StringClientPipeline, StackClient.defaultParams)

  val bridge: Future[Service[String, String]] =
    transporter(addr) map { transport =>
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
  val retry = new RetryExceptionsFilter[String, String](
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
