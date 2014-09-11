package com.twitter.finagle

import com.twitter.finagle.client.{StdStackClient, StackClient, Transporter}
import com.twitter.finagle.dispatch.{SerialClientDispatcher, SerialServerDispatcher}
import com.twitter.finagle.netty3.{Netty3Transporter, Netty3Listener}
import com.twitter.finagle.server.{StdStackServer, StackServer, Listener}
import com.twitter.finagle.thrift.{ClientId => _, _}
import com.twitter.finagle.transport.Transport
import com.twitter.util.Future
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

  val protocolFactory = Protocols.binaryFactory()
  protected val defaultClientName = "thrift"
  
  object param {
    case class ClientId(clientId: thrift.ClientId)
    implicit object ClientId extends Stack.Param[ClientId] {
      val default = ClientId(thrift.ClientId("unknown"))
    }
  }

  object Client {
    private val preparer = new Stack.Simple[ServiceFactory[ThriftClientRequest, Array[Byte]]] {
      val role = StackClient.Role.prepConn
      val description = "Prepare TTwitter thrift connection"
      def make(next: ServiceFactory[ThriftClientRequest, Array[Byte]])
          (implicit params: Params) = {
        val com.twitter.finagle.param.Label(label) = get[com.twitter.finagle.param.Label]
        val clientId = if (params.contains[param.ClientId]) {
          val param.ClientId(id) = get[param.ClientId]
          Some(id)
        } else None
  
        val preparer = new ThriftClientPreparer(protocolFactory, label, clientId)
        preparer.prepare(next)
      }
    }
    
    // We must do 'preparation' this way in order to let Finagle set up tracing & so on.
    val stack: Stack[ServiceFactory[ThriftClientRequest, Array[Byte]]] = StackClient.newStack
      .replace(StackClient.Role.prepConn, preparer)
  }

  case class Client(
    stack: Stack[ServiceFactory[ThriftClientRequest, Array[Byte]]] = Client.stack,
    params: Stack.Params = StackClient.defaultParams,
    protocolFactory: TProtocolFactory = Protocols.binaryFactory(),
    framed: Boolean = true
  ) extends StdStackClient[ThriftClientRequest, Array[Byte], Client] with ThriftRichClient {
    protected def copy1(
      stack: Stack[ServiceFactory[ThriftClientRequest, Array[Byte]]] = this.stack,
      params: Stack.Params = this.params
    ): Client = copy(stack, params)
    
    protected val defaultClientName = "thrift"

    protected type In = ThriftClientRequest
    protected type Out = Array[Byte]

    protected def newTransporter(): Transporter[In, Out] = {
      val pipeline = 
        if (framed) ThriftClientFramedPipelineFactory
        else ThriftClientBufferedPipelineFactory(protocolFactory)
      Netty3Transporter(pipeline, params)
    }

    protected def newDispatcher(
        transport: Transport[ThriftClientRequest, Array[Byte]]): Service[ThriftClientRequest, Array[Byte]] =
      new SerialClientDispatcher(transport)

    def withProtocolFactory(protocolFactory: TProtocolFactory): Client =
      copy(protocolFactory=protocolFactory)
    
    def withClientId(clientId: thrift.ClientId): Client =
      configured(param.ClientId(clientId))

    def clientId: Option[thrift.ClientId] =
      if (params.contains[param.ClientId])
        Some(params[param.ClientId].clientId)
      else
        None
  }


  val client = Client()

  def newClient(
    dest: Name, label: String
  ): ServiceFactory[ThriftClientRequest, Array[Byte]] = client.newClient(dest, label)

  @deprecated("Use client.withProtocolFactory", "6.22.0")
  def withProtocolFactory(protocolFactory: TProtocolFactory): Client =
    client.copy(protocolFactory=protocolFactory)

  @deprecated("Use client.withClientId", "6.22.0")
  def withClientId(clientId: thrift.ClientId): Client =
    client.configured(param.ClientId(clientId))

  object Server {
    private val preparer = new Stack.Simple[ServiceFactory[Array[Byte], Array[Byte]]] {
      val role = StackClient.Role.prepConn
      val description = "Prepare TTwitter thrift connection"
      def make(next: ServiceFactory[Array[Byte], Array[Byte]])(implicit params: Params) = {
        val com.twitter.finagle.param.Label(label) = get[com.twitter.finagle.param.Label]
        val preparer = new thrift.ThriftServerPreparer(protocolFactory, label)
        preparer.prepare(next)
      }
    }
    
    val stack: Stack[ServiceFactory[Array[Byte], Array[Byte]]] = StackServer.newStack
      .replace(StackServer.Role.preparer, preparer)
  }

  case class Server(
    stack: Stack[ServiceFactory[Array[Byte], Array[Byte]]] = Server.stack,
    params: Stack.Params = StackServer.defaultParams,
    protocolFactory: TProtocolFactory = Protocols.binaryFactory(),
    framed: Boolean = true
  ) extends StdStackServer[Array[Byte], Array[Byte], Server] with ThriftRichServer {
    protected def copy1(
      stack: Stack[ServiceFactory[Array[Byte], Array[Byte]]] = this.stack,
      params: Stack.Params = this.params
    ): Server = copy(stack, params)

    protected type In = Array[Byte]
    protected type Out = Array[Byte]
    
    protected def newListener(): Listener[In, Out] = {
      val pipeline =
        if (framed) thrift.ThriftServerFramedPipelineFactory
        else thrift.ThriftServerBufferedPipelineFactory(protocolFactory)

      Netty3Listener("thrift", pipeline)
    }

    protected def newDispatcher(transport: Transport[In, Out], service: Service[Array[Byte], Array[Byte]]) =
      new SerialServerDispatcher(transport, service)

    def withProtocolFactory(protocolFactory: TProtocolFactory): Server =
      copy(protocolFactory=protocolFactory)

    def withBufferedTransport(): Server = copy(framed=false)
  }

  val  server = Server()

  def serve(addr: SocketAddress, service: ServiceFactory[Array[Byte], Array[Byte]])
    : ListeningServer = server.serve(addr, service)
}
