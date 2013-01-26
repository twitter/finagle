package com.twitter.finagle

import java.net.SocketAddress
import com.twitter.finagle.client._
import com.twitter.finagle.dispatch.PipeliningDispatcher
import com.twitter.finagle.pool.ReusingPool
import com.twitter.finagle.netty3.Netty3Transporter
import com.twitter.finagle.builder.Cluster
import com.twitter.finagle.redis.protocol.{Command, Reply}

trait RedisRichClient { self: Client[Command, Reply] =>
  def newRichClient(cluster: Cluster[SocketAddress]): redis.Client = redis.Client(newClient(cluster).toService)
  def newRichClient(cluster: String): redis.Client = redis.Client(newClient(cluster).toService)
}

object RedisBinder extends DefaultBinder[Command, Reply, Command, Reply](
  new Netty3Transporter(redis.RedisClientPipelineFactory), 
  new PipeliningDispatcher(_)
)

object RedisClient extends DefaultClient[Command, Reply](
  RedisBinder, 
  _ => new ReusingPool(_)
) with RedisRichClient

object Redis extends Client[Command, Reply] with RedisRichClient {
  def newClient(cluster: Cluster[SocketAddress]): ServiceFactory[Command, Reply] =
    RedisClient.newClient(cluster)
}
