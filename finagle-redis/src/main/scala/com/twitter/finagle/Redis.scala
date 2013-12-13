package com.twitter.finagle

import com.twitter.finagle.client._
import com.twitter.finagle.dispatch.PipeliningDispatcher
import com.twitter.finagle.netty3.Netty3Transporter
import com.twitter.finagle.pool.ReusingPool
import com.twitter.finagle.redis.protocol.{Command, Reply}
import com.twitter.finagle.stats.StatsReceiver
import java.net.SocketAddress

trait RedisRichClient { self: Client[Command, Reply] =>
  @deprecated("Use destination names via newRichClient(String) or newRichClinet(Name)", "6.7.x")
  def newRichClient(group: Group[SocketAddress]): redis.Client = redis.Client(newClient(group).toService)
  
  def newRichClient(dest: String): redis.Client =
     redis.Client(newService(dest))

  def newRichClient(dest: Name, label: String): redis.Client =
    redis.Client(newService(dest, label))
}

object RedisTransporter extends Netty3Transporter[Command, Reply]("redis", redis.RedisClientPipelineFactory)

object RedisClient extends DefaultClient[Command, Reply](
  name = "redis",
  endpointer = Bridge[Command, Reply, Command, Reply](RedisTransporter, new PipeliningDispatcher(_)),
  pool = (sr: StatsReceiver) => new ReusingPool(_, sr)
) with RedisRichClient

object Redis extends Client[Command, Reply] with RedisRichClient {
  def newClient(dest: Name, label: String): ServiceFactory[Command, Reply] =
    RedisClient.newClient(dest, label)
}
