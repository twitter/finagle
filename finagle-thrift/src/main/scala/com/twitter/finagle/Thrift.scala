package com.twitter.finagle

import com.twitter.finagle.client.{StdStackClient, StackClient, Transporter}
import com.twitter.finagle.dispatch.{SerialClientDispatcher, SerialServerDispatcher}
import com.twitter.finagle.netty3.{Netty3Transporter, Netty3Listener}
import com.twitter.finagle.param.{Label, Stats, ProtocolLibrary}
import com.twitter.finagle.server.{StdStackServer, StackServer, Listener}
import com.twitter.finagle.thrift.{ClientId => _, _}
import com.twitter.finagle.transport.Transport
import com.twitter.util.Stopwatch
import java.net.SocketAddress
import org.apache.thrift.protocol.TProtocolFactory

/**
 * Client and server for [[http://thrift.apache.org Apache Thrift]].
 * `Thrift` implements Thrift framed transport and binary protocol by
 * default, though custom protocol factories (i.e. wire encoding) may
 * be injected with `withProtocolFactory`. The client,
 * `Client[ThriftClientRequest, Array[Byte]]` provides direct access
 * to the thrift transport, but we recommend using code generation
 * through either [[https://github.com/twitter/scrooge Scrooge]] or
 * [[https://github.com/mariusaeriksen/thrift-0.5.0-finagle a fork]]
 * of the Apache generator. A rich API is provided to support
 * interfaces generated with either of these code generators.
 *
 * The client and server uses the standard thrift protocols, with
 * support for both framed and buffered transports. Finagle attempts
 * to upgrade the protocol in order to ship an extra envelope
 * carrying additional request metadata, containing, among other
 * things, request IDs for Finagle's RPC tracing facilities.
 *
 * The negotiation is simple: on connection establishment, an
 * improbably-named method is dispatched on the server. If that
 * method isn't found, we are dealing with a legacy thrift server,
 * and the standard protocol is used. If the remote server is also a
 * finagle server (or any other supporting this extension), we reply
 * to the request, and every subsequent request is dispatched with an
 * envelope carrying trace metadata. The envelope itself is also a
 * Thrift struct described
 * [[https://github.com/twitter/finagle/blob/master/finagle-thrift/src/main/thrift/tracing.thrift
 * here]].
 *
 * == Clients ==
 *
 * $clientExample
 *
 * $thriftUpgrade
 *
 * == Servers ==
 *
 * $serverExample
 *
 * @define clientExampleObject Thrift
 * @define serverExampleObject Thrift
 */
object Thrift extends Client[ThriftClientRequest, Array[Byte]] with ThriftRichClient
    with Server[Array[Byte], Array[Byte]] with ThriftRichServer {

  val protocolFactory: TProtocolFactory = Protocols.binaryFactory()

  protected lazy val Label(defaultClientName) = client.params[Label]

  override protected lazy val Stats(stats) = client.params[Stats]

  object param {
    case class ClientId(clientId: Option[thrift.ClientId])
    implicit object ClientId extends Stack.Param[ClientId] {
      val default = ClientId(None)
    }

    case class ProtocolFactory(protocolFactory: TProtocolFactory)
    implicit object ProtocolFactory extends Stack.Param[ProtocolFactory] {
      val default = ProtocolFactory(Protocols.binaryFactory())
    }

    /**
     * A `Param` to set the max size of a reusable buffer for the thrift response.
     * If the buffer size exceeds the specified value, the buffer is not reused,
     * and a new buffer is used for the next thrift response.
     * @param maxReusableBufferSize Max buffer size in bytes.
     */
    case class MaxReusableBufferSize(maxReusableBufferSize: Int)
    implicit object MaxReusableBufferSize extends Stack.Param[MaxReusableBufferSize] {
      val default = MaxReusableBufferSize(maxThriftBufferSize)
    }
  }

  object Client {
    private val preparer: Stackable[ServiceFactory[ThriftClientRequest, Array[Byte]]] =
      new Stack.Module4[
        param.ClientId,
        Label,
        Stats,
        param.ProtocolFactory,
        ServiceFactory[ThriftClientRequest, Array[Byte]]
      ] {
        val role = StackClient.Role.prepConn
        val description = "Prepare TTwitter thrift connection"
        def make(
          _clientId: param.ClientId,
          _label: Label,
          _stats: Stats,
          _pf: param.ProtocolFactory,
          next: ServiceFactory[ThriftClientRequest, Array[Byte]]
        ) = {
          val Label(label) = _label
          val param.ClientId(clientId) = _clientId
          val param.ProtocolFactory(pf) = _pf
          val preparer = new ThriftClientPreparer(pf, label, clientId)
          val underlying = preparer.prepare(next)
          val Stats(stats) = _stats
          new ServiceFactoryProxy(underlying) {
            val stat = stats.stat("codec_connection_preparation_latency_ms")
            override def apply(conn: ClientConnection) = {
              val elapsed = Stopwatch.start()
              super.apply(conn) ensure {
                stat.add(elapsed().inMilliseconds)
              }
            }
          }
        }
      }

    // We must do 'preparation' this way in order to let Finagle set up tracing & so on.
    val stack: Stack[ServiceFactory[ThriftClientRequest, Array[Byte]]] = StackClient.newStack
      .replace(StackClient.Role.prepConn, preparer)
  }

  case class Client(
    stack: Stack[ServiceFactory[ThriftClientRequest, Array[Byte]]] = Client.stack,
    params: Stack.Params = StackClient.defaultParams + ProtocolLibrary("thrift"),
    framed: Boolean = true
  ) extends StdStackClient[ThriftClientRequest, Array[Byte], Client] with ThriftRichClient {
    protected def copy1(
      stack: Stack[ServiceFactory[ThriftClientRequest, Array[Byte]]] = this.stack,
      params: Stack.Params = this.params
    ): Client = copy(stack, params)

    protected lazy val Label(defaultClientName) = params[Label]

    protected type In = ThriftClientRequest
    protected type Out = Array[Byte]

    protected val param.ProtocolFactory(protocolFactory) = params[param.ProtocolFactory]
    override protected lazy val Stats(stats) = params[Stats]

    protected def newTransporter(): Transporter[In, Out] = {
      val pipeline =
        if (framed) ThriftClientFramedPipelineFactory
        else ThriftClientBufferedPipelineFactory(protocolFactory)
      Netty3Transporter(pipeline, params)
    }

    protected def newDispatcher(
      transport: Transport[ThriftClientRequest, Array[Byte]]
    ): Service[ThriftClientRequest, Array[Byte]] =
      new SerialClientDispatcher(transport)

    def withProtocolFactory(protocolFactory: TProtocolFactory): Client =
      configured(param.ProtocolFactory(protocolFactory))

    def withClientId(clientId: thrift.ClientId): Client =
      configured(param.ClientId(Some(clientId)))

    def clientId: Option[thrift.ClientId] = params[param.ClientId].clientId
  }


  val client = Client()
  
  def newService(
    dest: Name,
    label: String
  ): Service[ThriftClientRequest, Array[Byte]] = 
    client.newService(dest, label)

  def newClient(
    dest: Name,
    label: String
  ): ServiceFactory[ThriftClientRequest, Array[Byte]] = client.newClient(dest, label)

  @deprecated("Use `Thrift.client.withProtocolFactory`", "6.22.0")
  def withProtocolFactory(protocolFactory: TProtocolFactory): Client =
    client.withProtocolFactory(protocolFactory)

  @deprecated("Use `Thrift.client.withClientId`", "6.22.0")
  def withClientId(clientId: thrift.ClientId): Client =
    client.withClientId(clientId)

  object Server {
    private val preparer =
      new Stack.Module2[Label, param.ProtocolFactory, ServiceFactory[Array[Byte], Array[Byte]]]
    {
      val role = StackClient.Role.prepConn
      val description = "Prepare TTwitter thrift connection"
      def make(
        _label: Label,
        _pf: param.ProtocolFactory,
        next: ServiceFactory[Array[Byte], Array[Byte]]
      ) = {
        val Label(label) = _label
        val param.ProtocolFactory(pf) = _pf
        val preparer = new thrift.ThriftServerPreparer(pf, label)
        preparer.prepare(next)
      }
    }

    val stack: Stack[ServiceFactory[Array[Byte], Array[Byte]]] = StackServer.newStack
      .replace(StackServer.Role.preparer, preparer)
  }

  case class Server(
    stack: Stack[ServiceFactory[Array[Byte], Array[Byte]]] = Server.stack,
    params: Stack.Params = StackServer.defaultParams + ProtocolLibrary("thrift"),
    framed: Boolean = true
  ) extends StdStackServer[Array[Byte], Array[Byte], Server] with ThriftRichServer {
    protected def copy1(
      stack: Stack[ServiceFactory[Array[Byte], Array[Byte]]] = this.stack,
      params: Stack.Params = this.params
    ): Server = copy(stack, params)

    protected type In = Array[Byte]
    protected type Out = Array[Byte]

    protected val param.ProtocolFactory(protocolFactory) = params[param.ProtocolFactory]

    protected def newListener(): Listener[In, Out] = {
      val pipeline =
        if (framed) thrift.ThriftServerFramedPipelineFactory
        else thrift.ThriftServerBufferedPipelineFactory(protocolFactory)

      Netty3Listener(pipeline,
        if (params.contains[Label]) params else params + Label("thrift"))
    }

    protected def newDispatcher(
      transport: Transport[In, Out],
      service: Service[Array[Byte], Array[Byte]]
    ) =
      new SerialServerDispatcher(transport, service)

    def withProtocolFactory(protocolFactory: TProtocolFactory): Server =
      configured(param.ProtocolFactory(protocolFactory))

    def withBufferedTransport(): Server = copy(framed=false)
  }

  val server = Server()

  def serve(
    addr: SocketAddress,
    service: ServiceFactory[Array[Byte], Array[Byte]]
  ): ListeningServer = server.serve(addr, service)
}

