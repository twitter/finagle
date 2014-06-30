package com.twitter.finagle

import com.twitter.finagle.param.{Label, Stats}
import com.twitter.finagle.thrift.{ClientId, Protocols, ThriftClientRequest}
import java.net.SocketAddress
import com.twitter.finagle.stats.{ClientStatsReceiver, ServerStatsReceiver}
import org.apache.thrift.protocol.TProtocolFactory

/**
 * A default implementation of both ThriftMux clients and servers.
 * This class can't be instantiated, see [[com.twitter.finagle.ThriftMux]]
 * for the default instance.
 */
class ThriftMuxLike private[finagle](
  client: ThriftMuxClientLike,
  server: ThriftMuxServerLike,
  // TODO: consider stuffing this into Stack.Params
  protected val protocolFactory: TProtocolFactory
) extends Client[ThriftClientRequest, Array[Byte]] with ThriftRichClient
  with Server[Array[Byte], Array[Byte]] with ThriftRichServer
{
  protected lazy val defaultClientName = {
    val Label(label) = client.params[Label]
    label
  }

  override protected lazy val stats = {
    val Stats(sr) = client.params[Stats]
    sr
  }

  /** @inheritdoc */
  def newClient(dest: Name, label: String): ServiceFactory[ThriftClientRequest, Array[Byte]] =
    client.newClient(dest, label)

  /** @inheritdoc */
  def serve(
    addr: SocketAddress,
    service: ServiceFactory[Array[Byte], Array[Byte]]
  ): ListeningServer = server.serve(addr, service)

  /**
   * Produce a [[com.twitter.finagle.ThriftMuxLike]] using the provided
   * client ID.
   */
  def withClientId(clientId: ClientId): ThriftMuxLike =
    new ThriftMuxLike(client.withClientId(clientId), server, protocolFactory)

  /**
   * Produce a [[com.twitter.finagle.ThriftMuxLike]] using the provided
   * protocolFactory.
   */
  def withProtocolFactory(pf: TProtocolFactory): ThriftMuxLike =
    new ThriftMuxLike(
      client.withProtocolFactory(pf),
      server.withProtocolFactory(pf),
      pf
    )
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
 * This object does not expose any configuration options. Both clients and servers
 * are instantiated with sane defaults. Clients are labeled with the "clnt/thrift"
 * prefix and servers with "srv/thrift". If you'd like more configuration, see the
 * [[com.twitter.finagle.ThriftMuxServer]] and [[com.twitter.finagle.ThriftMuxClient]]
 * objects.
 *
 * @define clientExampleObject ThriftMux
 * @define serverExampleObject ThriftMux
 */
object ThriftMux extends ThriftMuxLike(
  ThriftMuxClient
    .configured(Stats(ClientStatsReceiver))
    .configured(Label("thrift")),
  ThriftMuxServer
    .configured(Stats(ServerStatsReceiver))
    .configured(Label("thrift")),
  Protocols.binaryFactory()
)