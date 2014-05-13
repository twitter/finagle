package com.twitter.finagle

import com.twitter.finagle.thrift.{ClientId, Protocols, ThriftClientRequest}
import com.twitter.util.{Future, Local, Time}
import org.apache.thrift.protocol.TProtocolFactory
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

/**
 * A [[com.twitter.finagle.client.Client]] for the Thrift protocol served over
 * [[com.twitter.finagle.mux]].
 *
 * $clientExample
 *
 * @define clientExampleObject ThriftMuxClientImpl(...)
 */
class ThriftMuxClientImpl(
  muxer: Client[ChannelBuffer, ChannelBuffer] = MuxClient,
  protected val protocolFactory: TProtocolFactory = Protocols.binaryFactory(),
  clientId: Option[ClientId] = None
) extends Client[ThriftClientRequest, Array[Byte]] with ThriftRichClient {
  protected val defaultClientName = "thrift"

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

/**
 * A client for thrift served over [[com.twitter.finagle.mux]]
 *
 * $clientExample
 *
 * @define clientExampleObject ThriftMuxClient
 */
object ThriftMuxClient extends ThriftMuxClientImpl(MuxClient)

package thriftmux.exp {
  /**
   * A client for thrift served over [[com.twitter.finagle.mux]]
   *
   * $clientExample
   *
   * @define clientExampleObject ThriftMuxClient
   */
  object ThriftMuxClient extends ThriftMuxClientImpl(MuxClient)
}
