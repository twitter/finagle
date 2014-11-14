package com.twitter.finagle

import com.twitter.finagle.Service
import com.twitter.finagle.client._
import com.twitter.finagle.mux.lease.Acting
import com.twitter.finagle.mux.lease.exp.Lessor
import com.twitter.finagle.netty3._
import com.twitter.finagle.pool.SingletonPool
import com.twitter.finagle.server._
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.tracing._
import com.twitter.util.Future
import java.net.SocketAddress
import org.jboss.netty.buffer.{ChannelBuffer => CB}

/**
 * A client and server for the mux protocol described in [[com.twitter.finagle.mux]].
 */
object Mux extends Client[CB, CB] with Server[CB, CB] {

  private[finagle] abstract class ProtoTracing(
    process: String,
    val role: Stack.Role
  ) extends Stack.Module0[ServiceFactory[CB, CB]] {
    val description = s"Mux specific $process traces"

    private[this] val tracingFilter = new SimpleFilter[CB, CB] {
      def apply(req: CB, svc: Service[CB, CB]): Future[CB] = {
        Trace.recordBinary(s"$process/mux/enabled", true)
        svc(req)
      }
    }

    def make(next: ServiceFactory[CB, CB]) =
      tracingFilter andThen next
  }

  private[finagle] class ClientProtoTracing extends ProtoTracing("clnt", StackClient.Role.protoTracing)

  object Client {
    val stack: Stack[ServiceFactory[CB, CB]] = StackClient.newStack
      .replace(StackClient.Role.pool, SingletonPool.module[CB, CB])
      .replace(StackClient.Role.prepConn, mux.lease.LeasedFactory.module[CB, CB])
      .replace(StackClient.Role.protoTracing, new ClientProtoTracing)
  }

  case class Client(
    stack: Stack[ServiceFactory[CB, CB]] = Client.stack,
    params: Stack.Params = StackClient.defaultParams
  ) extends StdStackClient[CB, CB, Client] {
    protected def copy1(
      stack: Stack[ServiceFactory[CB, CB]] = this.stack,
      params: Stack.Params = this.params
    ): Client = copy(stack, params)

    protected type In = CB
    protected type Out = CB

    protected def newTransporter(): Transporter[In, Out] =
      Netty3Transporter(mux.PipelineFactory, params)
    override protected def newDispatcher(transport: Transport[CB, CB]): Service[CB, CB] with Acting = {
      val param.Stats(sr) = params[param.Stats]
      new mux.ClientDispatcher(transport, sr)
    }
  }

  val client = Client()

  def newClient(dest: Name, label: String): ServiceFactory[CB, CB] =
    client.newClient(dest, label)

  private[finagle] class ServerProtoTracing extends ProtoTracing("srv", StackServer.Role.protoTracing)

  case class Server(
    stack: Stack[ServiceFactory[CB, CB]] = StackServer.newStack
      .remove(TraceInitializerFilter.role)
      .replace(StackServer.Role.protoTracing, new ServerProtoTracing),
    params: Stack.Params = StackServer.defaultParams
  ) extends StdStackServer[CB, CB, Server] {
    protected def copy1(
      stack: Stack[ServiceFactory[CB, CB]] = this.stack,
      params: Stack.Params = this.params
    ): Server = copy(stack, params)

    protected type In = CB
    protected type Out = CB

    protected def newListener(): Listener[In, Out] =
      Netty3Listener(mux.PipelineFactory, params)
    protected def newDispatcher(transport: Transport[In, Out], service: Service[CB, CB]) = {
      val param.Tracer(tracer) = params[param.Tracer]
      val Lessor.Param(lessor) = params[Lessor.Param]
      new mux.ServerDispatcher(transport, service, true, lessor, tracer)
    }
  }

  val server = Server()

  def serve(addr: SocketAddress, service: ServiceFactory[CB, CB]): ListeningServer =
    server.serve(addr, service)
}
