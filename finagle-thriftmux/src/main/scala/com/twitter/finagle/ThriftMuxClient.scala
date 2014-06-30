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
class ThriftMuxClientLike private[finagle](
  muxer: StackClient[CB, CB, CB, CB],
  // TODO: consider stuffing these into Stack.Params
  clientId: Option[ClientId],
  protected val protocolFactory: TProtocolFactory
) extends Client[ThriftClientRequest, Array[Byte]] with ThriftRichClient
  with (Stack.Params => Client[ThriftClientRequest, Array[Byte]]) {

  /**
   * The [[com.twitter.finagle.ServiceFactory]] stack that requests
   * are dispatched through.
   */
  val stack = muxer.stack

  /**
   * The [[com.twitter.finagle.Stack.Params]] used to configure
   * the stack.
   */
  val params = muxer.params

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
    new ThriftMuxClientLike(muxer.copy(params = params), clientId, protocolFactory)

  /**
   * Create a new ThriftMuxClientLike with `p` added to the
   * parameters used to configure the `muxer`.
   */
  def configured[P: Stack.Param](p: P): ThriftMuxClientLike =
    new ThriftMuxClientLike(muxer.configured(p), clientId, protocolFactory)

  /**
   * Produce a [[com.twitter.finagle.ThriftMuxClientLike]] using the provided
   * client ID.
   */
  def withClientId(clientId: ClientId): ThriftMuxClientLike =
    new ThriftMuxClientLike(muxer, Some(clientId), protocolFactory)

  /**
   * Produce a [[com.twitter.finagle.ThriftMuxClientLike]] using the provided
   * protocolFactory.
   */
  def withProtocolFactory(pf: TProtocolFactory): ThriftMuxClientLike =
    new ThriftMuxClientLike(muxer, clientId, pf)

  def newClient(dest: Name, label: String): ServiceFactory[ThriftClientRequest, Array[Byte]] =
    muxer.newClient(dest, label) map { service =>
      new Service[ThriftClientRequest, Array[Byte]] {
        def apply(req: ThriftClientRequest): Future[Array[Byte]] = {
          if (req.oneway) return Future.exception(
            new Exception("ThriftMux does not support one-way messages"))

          // We do a dance here to ensure that the proper ClientId is set when
          // `service` is applied because Mux relies on
          // com.twitter.finagle.thrift.ClientIdContext to propagate ClientIds.
          val save = Local.save()
          try {
            ClientId.set(clientId)
            service(ChannelBuffers.wrappedBuffer(req.message)) map { bytes =>
              ThriftMuxUtil.bufferToArray(bytes)
            }
          } finally {
            Local.restore(save)
          }
        }

        override def isAvailable = service.isAvailable
        override def close(deadline: Time) = service.close(deadline)
      }
    }
}

private[finagle] object ThriftMuxClientStack {
  def apply(): Stack[ServiceFactory[CB, CB]] =
    ThriftMuxUtil.protocolRecorder +: exp.MuxClient.stack
}

/**
 * A client for thrift served over [[com.twitter.finagle.mux]]
 *
 * $clientExample
 *
 * @define clientExampleObject ThriftMuxClient
 */
object ThriftMuxClient extends ThriftMuxClientLike(
  exp.MuxClient.copy(ThriftMuxClientStack(), Stack.Params.empty),
  None, Protocols.binaryFactory()
)