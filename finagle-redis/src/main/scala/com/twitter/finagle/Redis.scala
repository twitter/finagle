package com.twitter.finagle

import java.net.SocketAddress
import com.twitter.finagle.client._
import com.twitter.finagle.dispatch.PipeliningDispatcher
import com.twitter.finagle.pool.ReusingPool
import com.twitter.finagle.netty3.Netty3Transporter
import com.twitter.finagle.redis.protocol.{Command, Reply}

trait RedisRichClient { self: Client[Command, Reply] =>
  def newRichClient(group: Group[SocketAddress]): redis.Client = redis.Client(newClient(group).toService)
  def newRichClient(group: String): redis.Client = redis.Client(newClient(group).toService)
}

object RedisTransporter extends Netty3Transporter[Command, Reply](redis.RedisClientPipelineFactory)

object RedisClient extends DefaultClient[Command, Reply](
  name = "redis",
  endpointer = Bridge[Command, Reply, Command, Reply](RedisTransporter, new PipeliningDispatcher(_)),
  pool = _ => new ReusingPool(_)
) with RedisRichClient

object Redis extends Client[Command, Reply] with RedisRichClient {
  def newClient(group: Group[SocketAddress]): ServiceFactory[Command, Reply] =
    RedisClient.newClient(group)
}
