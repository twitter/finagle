package com.twitter.finagle

import com.twitter.finagle.thrift.{ClientId, Protocols, ThriftClientRequest}
import java.net.SocketAddress
import org.apache.thrift.protocol.TProtocolFactory

class ThriftMuxImpl private[finagle](
    clientId: Option[ClientId],
    val protocolFactory: TProtocolFactory)
  extends Client[ThriftClientRequest, Array[Byte]] with ThriftRichClient
  with Server[Array[Byte], Array[Byte]] with ThriftRichServer
{
  protected val defaultClientName = "thrift"

  private[this] val client =
    new ThriftMuxClientImpl(protocolFactory = protocolFactory, clientId = clientId)
  private[this] val server = new ThriftMuxServerImpl(protocolFactory = protocolFactory)

  def newClient(
    dest: Name,
    label: String
  ): ServiceFactory[ThriftClientRequest, Array[Byte]] = client.newClient(dest, label)

  def serve(
    addr: SocketAddress,
    service: ServiceFactory[Array[Byte], Array[Byte]]
  ): ListeningServer = server.serve(addr, service)

  /**
   * Produce a [[com.twitter.finagle.ThriftMuxImpl]] using the provided
   * client ID.
   */
  def withClientId(clientId: ClientId) = new ThriftMuxImpl(Some(clientId), protocolFactory)
}

/**
 * The `ThriftMux` object is both a [[com.twitter.finagle.client.Client]] and a
 * [[com.twitter.finagle.server.Server]] for the Thrift protocol served over
 * [[com.twitter.finagle.mux]]. Rich interfaces are provided to adhere to those
 * generated from a [[http://thrift.apache.org/docs/idl/ Thrift IDL]] by
 * [[http://twitter.github.io/scrooge/ Scrooge]] or
 * [[https://github.com/mariusaeriksen/thrift-0.5.0-finagle thrift-finagle]].
 *
 * Clients can be created directly from an interface generated from
 * a Thrift IDL:
 *
 * $clientExample
 *
 * Servers are also simple to expose:
 *
 * $serverExample
 *
 * By default, the Thrift binary protocol is used. Different protocol
 * factories may be supplied by instantiating new clients or servers.
 *
 * @define clientExampleObject ThriftMux
 * @define serverExampleObject ThriftMux
 */
object ThriftMux extends ThriftMuxImpl(None, Protocols.binaryFactory())
