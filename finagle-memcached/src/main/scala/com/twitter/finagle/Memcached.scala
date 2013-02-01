package com.twitter.finagle

import com.twitter.finagle.client._
import com.twitter.finagle.dispatch.{SerialServerDispatcher, PipeliningDispatcher}
import com.twitter.finagle.memcached.protocol.text.{
  MemcachedClientPipelineFactory, MemcachedServerPipelineFactory
}
import com.twitter.finagle.memcached.protocol.{Command, Response}
import com.twitter.finagle.netty3._
import com.twitter.finagle.pool.ReusingPool
import com.twitter.finagle.server._
import java.net.SocketAddress

trait MemcachedRichClient { self: Client[Command, Response] =>
  def newRichClient(group: Group[SocketAddress]): memcached.Client = memcached.Client(newClient(group).toService)
  def newRichClient(group: String): memcached.Client = memcached.Client(newClient(group).toService)
}

object MemcachedTransporter 
  extends Netty3Transporter[Command, Response](MemcachedClientPipelineFactory)

object MemcachedClient extends DefaultClient[Command, Response](
  name = "memcached",
  endpointer = Bridge[Command, Response, Command, Response](
    MemcachedTransporter, new PipeliningDispatcher(_)),
  pool = _ => new ReusingPool(_)
) with MemcachedRichClient

object MemcachedListener extends Netty3Listener[Response, Command](MemcachedServerPipelineFactory)
object MemcachedServer extends DefaultServer[Command, Response, Response, Command](
  "memcachedsrv", MemcachedListener, new SerialServerDispatcher(_, _)
)

object Memcached extends Client[Command, Response] with MemcachedRichClient with Server[Command, Response] {
  def newClient(group: Group[SocketAddress]): ServiceFactory[Command, Response] =
    MemcachedClient.newClient(group)

  def serve(addr: SocketAddress, service: ServiceFactory[Command, Response]): ListeningServer =
    MemcachedServer.serve(addr, service)
}
