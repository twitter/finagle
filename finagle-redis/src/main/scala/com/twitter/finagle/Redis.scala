package com.twitter.finagle

import java.net.SocketAddress
import com.twitter.finagle.client._
import com.twitter.finagle.builder.Cluster
import com.twitter.finagle.redis.protocol.{Command, Reply}
import com.twitter.finagle.netty3.{PipeliningTransport, Netty3Server}
import com.twitter.finagle.stats.StatsReceiver

class RedisTransport extends PipeliningTransport[Command, Reply](redis.RedisClientPipelineFactory)

class RedisClient(transport: ((SocketAddress, StatsReceiver)) => ServiceFactory[Command, Reply])
  extends DefaultClient[Command, Reply](DefaultClient.Config[Command, Reply](transport = transport))
{
  def this() = this(new RedisTransport)
  
  def newRichClient(cluster: Cluster[SocketAddress]): redis.Client = redis.Client(newClient(cluster).toService)
  def newRichClient(cluster: String): redis.Client = redis.Client(newClient(cluster).toService)
}

object Redis extends RedisClient {
  def defaultClient = this
}

