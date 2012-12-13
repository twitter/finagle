package com.twitter.finagle

import com.twitter.finagle.builder.Cluster
import com.twitter.finagle.client._
import com.twitter.finagle.netty3._
import com.twitter.finagle.pool.ReusingPool
import com.twitter.finagle.server._
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.ChannelTransport
import java.net.SocketAddress
import org.jboss.netty.buffer.ChannelBuffer

class MuxTransport extends DefaultTransport[ChannelBuffer, ChannelBuffer](
  DefaultTransport.Config[ChannelBuffer, ChannelBuffer](
    bind = Netty3Transport[ChannelBuffer, ChannelBuffer](
      Netty3Transport.Config(
        mux.PipelineFactory,
        newChannelDispatcher = new mux.ClientDispatcher(_),
        newChannelTransport = (ch, _) => new ChannelTransport[ChannelBuffer, ChannelBuffer](ch)
      )
    ),
    newPool = Function.const(new ReusingPool(_)))
)

class MuxClient(transport: ((SocketAddress, StatsReceiver)) => ServiceFactory[ChannelBuffer, ChannelBuffer])
  extends DefaultClient[ChannelBuffer, ChannelBuffer](
    DefaultClient.Config[ChannelBuffer, ChannelBuffer](transport = transport)
  )
{
  def this() = this(new MuxTransport)
}

class MuxNetty3Server extends Netty3Server[ChannelBuffer, ChannelBuffer](
  Netty3Server.Config(
    pipelineFactory = mux.PipelineFactory,
    newServerDispatcher = (newTransport, service) =>
      new mux.ServerDispatcher(newTransport(), service)
  )
)

class MuxServer extends DefaultServer[ChannelBuffer, ChannelBuffer](
  DefaultServer.Config[ChannelBuffer, ChannelBuffer](underlying = new MuxNetty3Server))

/**
 * A client and server for the mux protocol described in [[com.twitter.finagle.mux]].
 */
object Mux extends Client[ChannelBuffer, ChannelBuffer] with Server[ChannelBuffer, ChannelBuffer] {
  val defaultServer = new MuxServer
  val defaultClient = new MuxClient

  def newClient(cluster: Cluster[SocketAddress]): ServiceFactory[ChannelBuffer, ChannelBuffer] =
    defaultClient.newClient(cluster)

  def serve(addr: SocketAddress, service: ServiceFactory[ChannelBuffer, ChannelBuffer]): ListeningServer =
    defaultServer.serve(addr, service)
}
