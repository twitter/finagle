package com.twitter.finagle

import com.twitter.finagle.stats.{ClientStatsReceiver, StatsReceiver}
import com.twitter.finagle.thrift.ThriftClientRequest
import com.twitter.util.{Future, Time}
import java.net.SocketAddress
import org.apache.thrift.protocol.{TProtocolFactory, TBinaryProtocol}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}


/**
 * $clientExample
 *
 * @define clientExampleObject ThriftMuxClientImpl(...)
 */
case class ThriftMuxClientImpl(
  muxer: Client[ChannelBuffer, ChannelBuffer],
  protocolFactory: TProtocolFactory = new TBinaryProtocol.Factory()
) extends Client[ThriftClientRequest, Array[Byte]] with ThriftRichClient {
  protected val defaultClientName = "mux"

  def newClient(group: Group[SocketAddress]): ServiceFactory[ThriftClientRequest, Array[Byte]] =
    muxer.newClient(group) map { service =>
      new Service[ThriftClientRequest, Array[Byte]] {
        def apply(req: ThriftClientRequest): Future[Array[Byte]] = {
          if (req.oneway) return Future.exception(
            new Exception("ThriftMux does not support one-way messages"))

          service(ChannelBuffers.wrappedBuffer(req.message)) map
            ThriftMuxUtil.bufferToArray
        }
        override def isAvailable = service.isAvailable
        override def close(deadline: Time) = service.close(deadline)
      }
    }
}

/**
 * A client for thrift served over [[com.twitter.finagle.mux]]
 * $clientExample
 *
 * @define clientExampleObject ThriftMuxClient
 */
object ThriftMuxClient extends ThriftMuxClientImpl(MuxClient)
