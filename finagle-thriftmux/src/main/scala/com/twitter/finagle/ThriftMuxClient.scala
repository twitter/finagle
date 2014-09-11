package com.twitter.finagle

import com.twitter.finagle.client._
import com.twitter.finagle.param.{Label, Stats}
import com.twitter.finagle.thrift.{ClientId, Protocols, ThriftClientRequest}
import com.twitter.util.{Future, Local, Time}
import org.apache.thrift.protocol.TProtocolFactory
import org.jboss.netty.buffer.{ChannelBuffer => CB, ChannelBuffers}

/**
 * A [[com.twitter.finagle.Client]] for the Thrift protocol served over
 * [[com.twitter.finagle.mux]]. This class can't be instantiated. For a default
 * instance of ThriftMuxClientLike, see [[com.twitter.finagle.ThriftMuxClient]].
 */

@deprecated("Use object ThriftMux", "7.0.0")
class ThriftMuxClientLike private[finagle](client: ThriftMux.Client) 
    extends Client[ThriftClientRequest, Array[Byte]] with ThriftRichClient
    with (Stack.Params => Client[ThriftClientRequest, Array[Byte]]) {

  /**
   * The [[com.twitter.finagle.ServiceFactory]] stack that requests
   * are dispatched through.
   */
  def stack = client.stack

  /**
   * The [[com.twitter.finagle.Stack.Params]] used to configure
   * the stack.
   */
  def params = client.params
  
  protected val protocolFactory = client.protocolFactory

  protected lazy val defaultClientName = {
    val Label(label) = params[Label]
    label
  }

  override protected lazy val stats = {
    val Stats(sr) = params[Stats]
    sr
  }

  /**
   * Create a new ThriftMuxClientLike with `params` used to configure the
   * muxer. This makes `ThriftMuxClientLike` compatible with the legacy
   * [[com.twitter.finagle.builder.ClientBuilder]]. However, keep in mind
   * that many of the ClientBuilder parameters are not applicable to the
   * muxer.
   */
  def apply(params: Stack.Params): Client[ThriftClientRequest, Array[Byte]] =
    new ThriftMuxClientLike(client.withParams(params))

  /**
   * Create a new ThriftMuxClientLike with `p` added to the
   * parameters used to configure the `muxer`.
   */
  def configured[P: Stack.Param](p: P): ThriftMuxClientLike =
    new ThriftMuxClientLike(client.configured(p))

  /**
   * Produce a [[com.twitter.finagle.ThriftMuxClientLike]] using the provided
   * client ID.
   */
  def withClientId(clientId: ClientId): ThriftMuxClientLike =
    new ThriftMuxClientLike(client.withClientId(clientId))

  /**
   * Produce a [[com.twitter.finagle.ThriftMuxClientLike]] using the provided
   * protocolFactory.
   */
  def withProtocolFactory(pf: TProtocolFactory): ThriftMuxClientLike =
    new ThriftMuxClientLike(client.withProtocolFactory(pf))

  def newClient(dest: Name, label: String): ServiceFactory[ThriftClientRequest, Array[Byte]] =
    client.newClient(dest, label)
}

/**
 * A client for thrift served over [[com.twitter.finagle.mux]]
 *
 * $clientExample
 *
 * @define clientExampleObject ThriftMuxClient
 */
@deprecated("Use object ThriftMux", "7.0.0")
object ThriftMuxClient extends ThriftMuxClientLike(ThriftMux.client)
