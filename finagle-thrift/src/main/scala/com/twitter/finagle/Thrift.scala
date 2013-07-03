package com.twitter.finagle

import com.twitter.finagle.thrift.{ClientId, ThriftClientRequest}
import java.net.SocketAddress
import org.apache.thrift.protocol.{TBinaryProtocol, TProtocolFactory}

/**
 * Client and server for [[http://thrift.apache.org Apache Thrift]].
 * `Thrift` implements Thrift framed transport and binary protocol by
 * default, though custom protocol factories may be injected with
 * `withProtocolFactory`. The client, `Client[ThriftClientRequest,
 * Array[Byte]]` provides direct access to the thrift transport, but
 * we recommend using code generation though either
 * [[https://github.com/twitter/scrooge Scrooge]] or
 * [[https://github.com/mariusaeriksen/thrift-0.5.0-finagle a fork]]
 * of the Apache generator. A rich API is provided to support
 * interfaces generated with either of these code generators.
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
 * @note This class is not constructed directly: 
 * use it only through [[com.twitter.finagle.Thrift]]
 *
 * @define clientExampleObject ThriftImpl(..)
 * @define serverExampleObject ThriftImpl(..)
 *
 * @param protocolFactory The Apache Thrift ProtocolFactory with which
 * messages are encoded and decoded
 *
 * @param framed when true, use Apache Thrift's
 * [[http://people.apache.org/~jfarrell/thrift/0.6.1/javadoc/org/apache/thrift/transport/TFramedTransport.html framed transport]]
 *
 * @param clientId The client ID to be set in request header
 */
case class ThriftImpl private[finagle](
    protocolFactory: TProtocolFactory,
    framed: Boolean = true,
    clientId: Option[ClientId] = None)
  extends Client[ThriftClientRequest, Array[Byte]] with Server[Array[Byte], Array[Byte]]
  with ThriftRichClient with ThriftRichServer
{
  protected val defaultClientName = "thrift"

  private[this] val client = new thrift.ThriftClient(
    if (framed) thrift.ThriftFramedTransporter 
    else thrift.ThriftBufferedTransporter(protocolFactory),
    protocolFactory,
    clientId = clientId)

  private[this] val server = new thrift.ThriftServer(
    if (framed) thrift.ThriftFramedListener
    else thrift.ThriftBufferedListener(protocolFactory),
    protocolFactory)

  def newClient(
    group: Group[SocketAddress]
  ): ServiceFactory[ThriftClientRequest, Array[Byte]] = client.newClient(group)

  def serve(
    addr: SocketAddress, 
    service: ServiceFactory[Array[Byte], Array[Byte]]
  ): ListeningServer = server.serve(addr, service)


  /**
   * Produce a [[com.twitter.finagle.ThriftImpl]] using the given 
   * `TProtocolFactory` instead of the default binary protocol
   * factory.
   */
  def withProtocolFactory(pf: TProtocolFactory): ThriftImpl =
    copy(protocolFactory = pf)

  /**
   * Produce a [[com.twitter.finagle.ThriftImpl]] using
   * a buffered transport.
   *
   * @note Not recommended. Use only if absolutely required
   * for legacy purposes.
   */  
  def withBufferedTransport(): ThriftImpl =
    copy(framed = false)

  /**
   * Produce a [[com.twitter.finagle.ThriftImpl]] using
   * the provided client ID.
   */  
  def withClientId(clientId: ClientId): ThriftImpl =
    copy(clientId = Some(clientId))
}

/**
 * Client and server for [[http://thrift.apache.org Apache Thrift]].
 * `Thrift` implements Thrift framed transport and binary protocol by
 * default, though custom protocol factories may be injected with
 * `withProtocolFactory`. The client, `Client[ThriftClientRequest,
 * Array[Byte]]` provides direct access to the thrift transport, but
 * we recommend using code generation though either
 * [[https://github.com/twitter/scrooge Scrooge]] or
 * [[https://github.com/mariusaeriksen/thrift-0.5.0-finagle a fork]]
 * of the Apache generator. A rich API is provided to support
 * interfaces generated with either of these code generators.
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
object Thrift 
  extends Client[ThriftClientRequest, Array[Byte]] 
  with Server[Array[Byte], Array[Byte]]
  with ThriftRichClient with ThriftRichServer {

  val protocolFactory = new TBinaryProtocol.Factory()
  protected val defaultClientName = "thrift"
  private[this] val thrift = ThriftImpl(protocolFactory)

  def newClient(
    group: Group[SocketAddress]
  ): ServiceFactory[ThriftClientRequest, Array[Byte]] = thrift.newClient(group)

  def serve(
    addr: SocketAddress, service: ServiceFactory[Array[Byte], Array[Byte]]
  ): ListeningServer = thrift.serve(addr, service)

  /**
   * Produce a [[com.twitter.finagle.ThriftImpl]] using the given 
   * `TProtocolFactory` instead of the default binary protocol
   * factory.
   */
  def withProtocolFactory(pf: TProtocolFactory): ThriftImpl =
    thrift.withProtocolFactory(pf)

  /**
   * Produce a [[com.twitter.finagle.ThriftImpl]] using
   * a buffered transport.
   *
   * @note Not recommended. Use only if absolutely required
   * for legacy purposes.
   */
  def withBufferedTransport(): ThriftImpl =
    thrift.withBufferedTransport()

  /**
   * Produce a [[com.twitter.finagle.ThriftImpl]] using
   * the provided client ID.
   */  
  def withClientId(clientId: ClientId): ThriftImpl =
    thrift.withClientId(clientId)
}
