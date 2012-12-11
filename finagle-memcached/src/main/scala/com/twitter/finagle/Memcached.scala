package com.twitter.finagle

import java.net.SocketAddress
import com.twitter.finagle.client._
import com.twitter.finagle.server._
import com.twitter.finagle.builder.Cluster
import com.twitter.finagle.memcached.protocol.{Command, Response}
import com.twitter.finagle.memcached.protocol.text.{MemcachedClientPipelineFactory, MemcachedServerPipelineFactory}
import com.twitter.finagle.netty3.{PipeliningTransport, Netty3Server}
import com.twitter.finagle.stats.StatsReceiver


class MemcachedTransport 
  extends PipeliningTransport[Command, Response](MemcachedClientPipelineFactory)

trait MemcachedRichClient { self: Client[Command, Response] =>
  def newRichClient(cluster: Cluster[SocketAddress]): memcached.Client = memcached.Client(newClient(cluster).toService)
  def newRichClient(cluster: String): memcached.Client = memcached.Client(newClient(cluster).toService)
}

class MemcachedClient(transport: ((SocketAddress, StatsReceiver)) => ServiceFactory[Command, Response])
  extends DefaultClient[Command, Response](DefaultClient.Config[Command, Response](transport = transport))
  with MemcachedRichClient
{
  def this() = this(new MemcachedTransport)
}

class MemcachedNetty3Server extends Netty3Server[Command, Response](
  Netty3Server.Config(pipelineFactory = MemcachedServerPipelineFactory))

class MemcachedServer extends DefaultServer[Command, Response](
  DefaultServer.Config[Command, Response](underlying = new MemcachedNetty3Server))

object Memcached extends Client[Command, Response] with MemcachedRichClient with Server[Command, Response] {
  val defaultClient = new MemcachedClient
  val defaultServer = new MemcachedServer
  def newClient(cluster: Cluster[SocketAddress]): ServiceFactory[Command, Response] = 
    defaultClient.newClient(cluster)
  def serve(addr: SocketAddress, service: ServiceFactory[Command, Response]): ListeningServer =
    defaultServer.serve(addr, service)
}
