package com.twitter.finagle

import java.net.SocketAddress
import org.apache.thrift.protocol.TProtocolFactory

/**
 * A server for the Thrift protocol served over [[com.twitter.finagle.mux]].
 * ThriftMuxServer is backwards-compatible with Thrift clients that use the
 * framed transport and binary protocol. It switches to the backward-compatible
 * mode when the first request is not recognized as a valid Mux message but can
 * be successfully handled by the underlying Thrift service. Since a Thrift
 * message that is encoded with the binary protocol starts with a header value of
 * 0x800100xx, Mux does not confuse it with a valid Mux message (0x80 = -128 is
 * an invalid Mux message type) and the server can reliably detect the non-Mux
 * Thrift client and switch to the backwards-compatible mode.
 *
 * Note that the server is also compatible with non-Mux finagle-thrift clients.
 * It correctly responds to the protocol up-negotiation request and passes the
 * tracing information embedded in the thrift requests to Mux (which has native
 * tracing support).
 *
 * This class can't be instantiated. For a default instance of ThriftMuxServerLike,
 * see [[com.twitter.finagle.ThriftMuxServer]]
 */
@deprecated("Use object ThriftMux", "7.0.0")
class ThriftMuxServerLike private[finagle](server: ThriftMux.Server)
  extends Server[Array[Byte], Array[Byte]] with ThriftRichServer
  with (Stack.Params => Server[Array[Byte], Array[Byte]])
{
  /**
   * Used for Java access.
   */
  def get() = this

  /**
   * The [[com.twitter.finagle.ServiceFactory]] stack that requests
   * are dispatched through.
   */
  def stack = server.stack

  /**
   * The [[com.twitter.finagle.Stack.Params]] used to configure
   * the stack.
   */
  def params = server.params

  protected val Thrift.param.ProtocolFactory(protocolFactory) =
    server.params[Thrift.param.ProtocolFactory]

  /**
   * Create a new ThriftMuxServerLike with `params` used to configure the
   * muxer. This makes `ThriftMuxServerLike` compatible with the legacy
   * [[com.twitter.finagle.builder.ServerBuilder]].
   */
  def apply(params: Stack.Params): Server[Array[Byte], Array[Byte]] =
    new ThriftMuxServerLike(server.withParams(this.params ++ params))

  /**
   * Create a new ThriftMuxServerLike with `p` added to the
   * parameters used to configure the `muxer`.
   */
  def configured[P: Stack.Param](p: P): ThriftMuxServerLike =
    new ThriftMuxServerLike(server.configured(p))

  /**
   * Produce a [[com.twitter.finagle.ThriftMuxServerLike]] using the provided
   * protocolFactory.
   */
  def withProtocolFactory(pf: TProtocolFactory): ThriftMuxServerLike =
    new ThriftMuxServerLike(server.withProtocolFactory(pf))

  def serve(addr: SocketAddress, factory: ServiceFactory[Array[Byte], Array[Byte]]) =
    server.serve(addr, factory)
}

/**
 * A Thrift server served over [[com.twitter.finagle.mux]].
 *
 * $serverExample
 *
 * @define serverExampleObject ThriftMuxServer
 */
@deprecated("Use object ThriftMux", "7.0.0")
object ThriftMuxServer extends ThriftMuxServerLike(ThriftMux.server)
