package com.twitter.finagle

import com.twitter.app.GlobalFlag
import com.twitter.conversions.storage._
import com.twitter.finagle.client._
import com.twitter.finagle.factory.BindingFactory
import com.twitter.finagle.filter.PayloadSizeFilter
import com.twitter.finagle.mux.lease.exp.Lessor
import com.twitter.finagle.mux.transport.{Message, MuxFramer, Netty3Framer}
import com.twitter.finagle.mux.{Handshake, FailureDetector}
import com.twitter.finagle.netty3._
import com.twitter.finagle.param.{WithDefaultLoadBalancer, ProtocolLibrary}
import com.twitter.finagle.pool.SingletonPool
import com.twitter.finagle.server._
import com.twitter.finagle.tracing._
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Closable, Future, StorageUnit}
import java.net.SocketAddress
import org.jboss.netty.buffer.ChannelBuffer

package mux {
  /**
   * Experimental support for mux payload framing.
   */
  object maxFrameSize extends GlobalFlag[StorageUnit](
    Int.MaxValue.bytes,
    "The maximum size of a mux frame. Any message that is larger "+
    "than this value is fragmented across multiple transmissions.")
}

/**
 * A client and server for the mux protocol described in [[com.twitter.finagle.mux]].
 */
object Mux extends Client[mux.Request, mux.Response] with Server[mux.Request, mux.Response] {
  /**
   * The current version of the mux protocol.
   */
  val LatestVersion: Short = 0x0001

  /**
   * Extract feature flags from `headers` and decorate `trans`.
   */
  private[finagle] val negotiate: Handshake.Negotiator = (headers, trans) => {
    val window = Handshake.valueOf(MuxFramer.Header.KeyBuf, headers)
      .map { cb => MuxFramer.Header.decodeFrameSize(cb) }
    if (window.nonEmpty && window.get < Int.MaxValue) {
      MuxFramer(trans, window.get)
    } else {
      trans.map(Message.encode, Message.decode)
    }
  }

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

  object Client {
    /** Prepends bound residual paths to outbound Mux requests's destinations. */
    private object MuxBindingFactory extends BindingFactory.Module[mux.Request, mux.Response] {
      protected[this] def boundPathFilter(residual: Path) =
        Filter.mk[mux.Request, mux.Response, mux.Request, mux.Response] { (req, service) =>
          service(mux.Request(residual ++ req.destination, req.body))
        }
    }

    val stack: Stack[ServiceFactory[mux.Request, mux.Response]] = StackClient.newStack
      .replace(StackClient.Role.pool, SingletonPool.module[mux.Request, mux.Response])
      .replace(StackClient.Role.protoTracing, new ClientProtoTracing)
      .replace(BindingFactory.role, MuxBindingFactory)
      .prepend(PayloadSizeFilter.module(_.body.length, _.body.length))

    private def headers: Handshake.Headers = Seq(
      MuxFramer.Header.KeyBuf -> MuxFramer.Header.encodeFrameSize(
        mux.maxFrameSize().inBytes.toInt)
    )
  }

  case class Client(
      stack: Stack[ServiceFactory[mux.Request, mux.Response]] = Client.stack,
      params: Stack.Params = StackClient.defaultParams + ProtocolLibrary("mux"))
    extends StdStackClient[mux.Request, mux.Response, Client]
    with WithDefaultLoadBalancer[Client] {

    protected def copy1(
      stack: Stack[ServiceFactory[mux.Request, mux.Response]] = this.stack,
      params: Stack.Params = this.params
    ): Client = copy(stack, params)

    protected type In = ChannelBuffer
    protected type Out = ChannelBuffer

    protected def newTransporter(): Transporter[In, Out] =
      Netty3Transporter(Netty3Framer, params)

    protected def newDispatcher(
      transport: Transport[In, Out]
    ): Service[mux.Request, mux.Response] = {
      val param.Stats(sr) = params[param.Stats]
      val param.Label(name) = params[param.Label]

      val FailureDetector.Param(detectorConfig) = params[FailureDetector.Param]

      val negotiatedTrans = mux.Handshake.client(
        trans = transport,
        version = LatestVersion,
        headers = Client.headers,
        negotiate = negotiate)

      val session = new mux.ClientSession(
        negotiatedTrans,
        detectorConfig,
        name,
        sr.scope("mux"))

      mux.ClientDispatcher.newRequestResponse(session)
    }
  }

  val client = Client()

  def newService(dest: Name, label: String): Service[mux.Request, mux.Response] =
    client.newService(dest, label)

  def newClient(dest: Name, label: String): ServiceFactory[mux.Request, mux.Response] =
    client.newClient(dest, label)

  private[finagle] class ServerProtoTracing extends ProtoTracing("srv", StackServer.Role.protoTracing)

  object Server {
    val stack: Stack[ServiceFactory[mux.Request, mux.Response]] = StackServer.newStack
      .remove(TraceInitializerFilter.role)
      .replace(StackServer.Role.protoTracing, new ServerProtoTracing)
      .prepend(PayloadSizeFilter.module(_.body.length, _.body.length))

    /**
     * Determine the server headers based on `clientHeaders`.
     */
    private[finagle] def headers(clientHeaders: Handshake.Headers): Handshake.Headers = {
      val frameSize = Handshake.valueOf(MuxFramer.Header.KeyBuf, clientHeaders)
      if (frameSize.isEmpty) Nil
      else Seq(MuxFramer.Header.KeyBuf -> MuxFramer.Header.encodeFrameSize(
        mux.maxFrameSize().inBytes.toInt))
    }
  }

  case class Server(
      stack: Stack[ServiceFactory[mux.Request, mux.Response]] = Server.stack,
      params: Stack.Params = StackServer.defaultParams + ProtocolLibrary("mux"))
    extends StdStackServer[mux.Request, mux.Response, Server] {

    protected def copy1(
      stack: Stack[ServiceFactory[mux.Request, mux.Response]] = this.stack,
      params: Stack.Params = this.params
    ): Server = copy(stack, params)

    protected type In = ChannelBuffer
    protected type Out = ChannelBuffer

    private[this] val statsReceiver = {
      val param.Stats(statsReceiver) = params[param.Stats]
      statsReceiver.scope("mux")
    }

    protected def newListener(): Listener[In, Out] =
      Netty3Listener(Netty3Framer, params)

    protected def newDispatcher(
      transport: Transport[In, Out],
      service: Service[mux.Request, mux.Response]
    ): Closable = {
      val param.Tracer(tracer) = params[param.Tracer]
      val Lessor.Param(lessor) = params[Lessor.Param]

      val negotiatedTrans = mux.Handshake.server(
        trans = transport,
        version = LatestVersion,
        headers = Server.headers,
        negotiate = negotiate)

      mux.ServerDispatcher.newRequestResponse(
        negotiatedTrans,
        service,
        lessor,
        tracer,
        statsReceiver)
    }
  }

  val server = Server()

  def serve(
    addr: SocketAddress,
    service: ServiceFactory[mux.Request, mux.Response]
  ): ListeningServer = server.serve(addr, service)
}
