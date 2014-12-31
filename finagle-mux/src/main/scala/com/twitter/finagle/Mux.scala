package com.twitter.finagle

import com.twitter.finagle.client._
import com.twitter.finagle.factory.BindingFactory
import com.twitter.finagle.mux.lease.exp.Lessor
import com.twitter.finagle.netty3._
import com.twitter.finagle.pool.SingletonPool
import com.twitter.finagle.server._
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.tracing._
import com.twitter.finagle.transport.Transport
import com.twitter.util.Future
import java.net.SocketAddress
import org.jboss.netty.buffer.{ChannelBuffer => CB}

/**
 * A client and server for the mux protocol described in [[com.twitter.finagle.mux]].
 */
object Mux extends Client[mux.Request, mux.Response] with Server[mux.Request, mux.Response] {

  private[finagle] abstract class ProtoTracing(
    process: String,
    val role: Stack.Role
  ) extends Stack.Module0[ServiceFactory[mux.Request, mux.Response]] {
    val description = s"Mux specific $process traces"

    private[this] val tracingFilter = new SimpleFilter[mux.Request, mux.Response] {
      def apply(req: mux.Request, svc: Service[mux.Request, mux.Response]): Future[mux.Response] = {
        Trace.recordBinary(s"$process/mux/enabled", true)
        svc(req)
      }
    }

    def make(next: ServiceFactory[mux.Request, mux.Response]) =
      tracingFilter andThen next
  }

  private[finagle] class ClientProtoTracing extends ProtoTracing("clnt", StackClient.Role.protoTracing)

  /** Prepends bound residual paths to outbound Mux requests's destinations. */
  private[finagle] object MuxBindingFactory
    extends BindingFactory.Module[mux.Request, mux.Response] {

    protected[this] def boundPathFilter(residual: Path) =
      Filter.mk[mux.Request, mux.Response, mux.Request, mux.Response] { (req, service) =>
        service(mux.Request(residual ++ req.destination, req.body))
      }
  }

  object Client {
    val stack: Stack[ServiceFactory[mux.Request, mux.Response]] = StackClient.newStack
      .replace(StackClient.Role.pool, SingletonPool.module[mux.Request, mux.Response])
      .replace(StackClient.Role.protoTracing, new ClientProtoTracing)
      .replace(BindingFactory.role, MuxBindingFactory)
  }

  case class Client(
    stack: Stack[ServiceFactory[mux.Request, mux.Response]] = Client.stack,
    params: Stack.Params = StackClient.defaultParams
  ) extends StdStackClient[mux.Request, mux.Response, Client] {
    protected def copy1(
      stack: Stack[ServiceFactory[mux.Request, mux.Response]] = this.stack,
      params: Stack.Params = this.params
    ): Client = copy(stack, params)

    protected type In = CB
    protected type Out = CB

    protected def newTransporter(): Transporter[In, Out] =
      Netty3Transporter(mux.PipelineFactory, params)
    override protected def newDispatcher(
      transport: Transport[CB, CB]
    ): Service[mux.Request, mux.Response] = {
      val param.Stats(sr) = params[param.Stats]
      val param.Label(name) = params[param.Label]
      new mux.ClientDispatcher(name, transport, sr)
    }
  }

  val client = Client()

  def newClient(dest: Name, label: String): ServiceFactory[mux.Request, mux.Response] =
    client.newClient(dest, label)

  private[finagle] class ServerProtoTracing extends ProtoTracing("srv", StackServer.Role.protoTracing)

  case class Server(
    stack: Stack[ServiceFactory[mux.Request, mux.Response]] = StackServer.newStack
      .remove(TraceInitializerFilter.role)
      .replace(StackServer.Role.protoTracing, new ServerProtoTracing),
    params: Stack.Params = StackServer.defaultParams
  ) extends StdStackServer[mux.Request, mux.Response, Server] {
    protected def copy1(
      stack: Stack[ServiceFactory[mux.Request, mux.Response]] = this.stack,
      params: Stack.Params = this.params
    ): Server = copy(stack, params)

    protected type In = CB
    protected type Out = CB

    protected def newListener(): Listener[In, Out] =
      Netty3Listener(mux.PipelineFactory, params)
    protected def newDispatcher(
      transport: Transport[In, Out],
      service: Service[mux.Request, mux.Response]
    ) = {
      val param.Tracer(tracer) = params[param.Tracer]
      val Lessor.Param(lessor) = params[Lessor.Param]
      new mux.ServerDispatcher(transport, service, true, lessor, tracer)
    }
  }

  val server = Server()

  def serve(
    addr: SocketAddress,
    service: ServiceFactory[mux.Request, mux.Response]
  ): ListeningServer = server.serve(addr, service)
}
