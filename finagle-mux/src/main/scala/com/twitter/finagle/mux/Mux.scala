package com.twitter.finagle

import com.twitter.finagle.client._
import com.twitter.finagle.netty3._
import com.twitter.finagle.pool.ReusingPool
import com.twitter.finagle.server._
import java.net.SocketAddress
import org.jboss.netty.buffer.ChannelBuffer

object MuxBinder extends DefaultBinder[ChannelBuffer, ChannelBuffer, ChannelBuffer, ChannelBuffer](
  new Netty3Transporter(mux.PipelineFactory),
  new mux.ClientDispatcher(_)
)
object MuxClient extends DefaultClient[ChannelBuffer, ChannelBuffer](MuxBinder, _ => new ReusingPool(_))

object MuxListener extends Netty3Listener[ChannelBuffer, ChannelBuffer](mux.PipelineFactory)
object MuxServer extends DefaultServer[ChannelBuffer, ChannelBuffer, ChannelBuffer, ChannelBuffer](
  MuxListener, new mux.ServerDispatcher(_, _))

/**
 * A client and server for the mux protocol described in [[com.twitter.finagle.mux]].
 */
object Mux extends Client[ChannelBuffer, ChannelBuffer] with Server[ChannelBuffer, ChannelBuffer] {
  def newClient(group: Group[SocketAddress]): ServiceFactory[ChannelBuffer, ChannelBuffer] =
    MuxClient.newClient(group)

  def serve(addr: SocketAddress, service: ServiceFactory[ChannelBuffer, ChannelBuffer]): ListeningServer =
    MuxServer.serve(addr, service)
}
