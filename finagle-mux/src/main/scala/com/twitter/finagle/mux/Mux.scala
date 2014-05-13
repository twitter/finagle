package com.twitter.finagle

import com.twitter.finagle.client._
import com.twitter.finagle.netty3._
import com.twitter.finagle.pool.ReusingPool
import com.twitter.finagle.server._
import com.twitter.finagle.stack.Endpoint
import com.twitter.finagle.stats.{ClientStatsReceiver, StatsReceiver}
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Closable, CloseAwaitably, Future, Promise, Return, Time}
import java.net.SocketAddress
import org.jboss.netty.buffer.ChannelBuffer

object MuxTransporter extends Netty3Transporter[ChannelBuffer, ChannelBuffer](
  "mux", mux.PipelineFactory)

object MuxClient extends DefaultClient[ChannelBuffer, ChannelBuffer](
  name = "mux",
  endpointer = (sa, sr) =>
    (mux.lease.LeasedBridge[ChannelBuffer, ChannelBuffer, ChannelBuffer, ChannelBuffer](
      MuxTransporter, new mux.ClientDispatcher(_, sr))(sa, sr)),
  pool = (sr: StatsReceiver) => new ReusingPool(_, sr.scope("reusingpool")))

object MuxListener extends Netty3Listener[ChannelBuffer, ChannelBuffer]("mux", mux.PipelineFactory)
object MuxServer extends DefaultServer[ChannelBuffer, ChannelBuffer, ChannelBuffer, ChannelBuffer](
  "mux", MuxListener, new mux.ServerDispatcher(_, _, true)
)

/**
 * A client and server for the mux protocol described in [[com.twitter.finagle.mux]].
 */
object Mux extends Client[ChannelBuffer, ChannelBuffer] with Server[ChannelBuffer, ChannelBuffer] {
  def newClient(dest: Name, label: String): ServiceFactory[ChannelBuffer, ChannelBuffer] =
    MuxClient.newClient(dest, label)

  def serve(addr: SocketAddress, service: ServiceFactory[ChannelBuffer, ChannelBuffer]): ListeningServer =
    MuxServer.serve(addr, service)

  object MuxClient extends StackClient[ChannelBuffer, ChannelBuffer, ChannelBuffer, ChannelBuffer](
    StackClient.newStack
      .replace(StackClient.Role.Pool, ReusingPool.module[ChannelBuffer, ChannelBuffer])
      // if LeasedFactory.module is not directly after the endpointer, the Stack will break
      .replace(
        StackClient.Role.PrepConn, mux.lease.LeasedFactory.module[ChannelBuffer, ChannelBuffer]
      ),
    Stack.Params.empty
  ) {
    protected val newTransporter: Stack.Params => Transporter[ChannelBuffer, ChannelBuffer] = { prms =>
      Netty3Transporter(mux.PipelineFactory, prms)
    }

    protected val newDispatcher: Stack.Params =>
      Transport[ChannelBuffer, ChannelBuffer] =>
      Service[ChannelBuffer, ChannelBuffer] with mux.lease.Acting = { prms =>
      val param.Stats(sr) = prms[param.Stats]
      trans => new mux.ClientDispatcher(trans.cast[ChannelBuffer, ChannelBuffer], sr)
    }
  }
}
