package com.twitter.finagle

import com.twitter.finagle.client._
import com.twitter.finagle.netty3._
import com.twitter.finagle.pool.ReusingPool
import com.twitter.finagle.server._
import java.net.SocketAddress
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.stats.StatsReceiver

object MuxTransporter extends Netty3Transporter[ChannelBuffer, ChannelBuffer](
  "mux", mux.PipelineFactory)

object MuxClient extends DefaultClient[ChannelBuffer, ChannelBuffer](
  name = "mux",
  endpointer = Bridge[ChannelBuffer, ChannelBuffer, ChannelBuffer, ChannelBuffer](
    MuxTransporter, new mux.ClientDispatcher(_)),
  pool = (sr: StatsReceiver) => new ReusingPool(_, sr.scope("reusingpool"))
)

object MuxListener extends Netty3Listener[ChannelBuffer, ChannelBuffer]("mux", mux.PipelineFactory)
object MuxServer extends DefaultServer[ChannelBuffer, ChannelBuffer, ChannelBuffer, ChannelBuffer](
  "mux", MuxListener, new mux.ServerDispatcher(_, _)
)

/**
 * A client and server for the mux protocol described in [[com.twitter.finagle.mux]].
 */
object Mux extends Client[ChannelBuffer, ChannelBuffer] with Server[ChannelBuffer, ChannelBuffer] {
  def newClient(group: Group[SocketAddress]): ServiceFactory[ChannelBuffer, ChannelBuffer] =
    MuxClient.newClient(group)

  def serve(addr: SocketAddress, service: ServiceFactory[ChannelBuffer, ChannelBuffer]): ListeningServer =
    MuxServer.serve(addr, service)
}
