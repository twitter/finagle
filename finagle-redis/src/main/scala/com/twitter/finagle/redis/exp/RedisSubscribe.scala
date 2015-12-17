package com.twitter.finagle.redis.exp

import com.twitter.finagle._
import com.twitter.finagle.client.{DefaultPool, StackClient, StdStackClient, Transporter}
import com.twitter.finagle.netty3.Netty3Transporter
import com.twitter.finagle.pool.SingletonPool
import com.twitter.finagle.redis.protocol.{Reply, SubscribeCommand}
import com.twitter.finagle.transport.Transport

trait RichSubscribeClient { self: Client[SubscribeCommand, Unit] =>

  def newRichClient(dest: String) = SubscribeClient(dest)
}

object RedisSubscribe extends Client[SubscribeCommand, Unit] {

  object SubscribeClient {
    /**
     * Default stack parameters used for redis client.
     */
    val defaultParams: Stack.Params = StackClient.defaultParams +
      param.ProtocolLibrary("redis.subscribe")

    /**
     * A default client stack which supports the pipelined redis client.
     */
    def newStack: Stack[ServiceFactory[SubscribeCommand, Unit]] = StackClient.newStack
      .replace(DefaultPool.Role, SingletonPool.module[SubscribeCommand, Unit])
  }

  case class SubscribeClient(
    stack: Stack[ServiceFactory[SubscribeCommand, Unit]] = SubscribeClient.newStack,
    params: Stack.Params = SubscribeClient.defaultParams)
      extends StdStackClient[SubscribeCommand, Unit, SubscribeClient]
      with RichSubscribeClient {

    protected def copy1(
      stack: Stack[ServiceFactory[SubscribeCommand, Unit]] = this.stack,
      params: Stack.Params = this.params): SubscribeClient = copy(stack, params)

    protected type In = SubscribeCommand
    protected type Out = Reply

    protected def newTransporter(): Transporter[In, Out] =
      Netty3Transporter(redis.RedisClientPipelineFactory, params)

    protected def newDispatcher(transport: Transport[In, Out]): Service[SubscribeCommand, Unit] =
      new SubscribeDispatcher(transport)
  }

  val client = SubscribeClient()

  def newClient(dest: Name, label: String): ServiceFactory[SubscribeCommand, Unit] =
    client.newClient(dest, label)

  def newService(dest: Name, label: String): Service[SubscribeCommand, Unit] =
    client.newService(dest, label)
}
