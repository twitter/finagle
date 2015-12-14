package com.twitter.finagle

import com.twitter.finagle.client._
import com.twitter.finagle.dispatch.PipeliningDispatcher
import com.twitter.finagle.loadbalancer._
import com.twitter.finagle.netty3.Netty3Transporter
import com.twitter.finagle.pool.SingletonPool
import com.twitter.finagle.redis.dispatch.SubscribeDispatcher
import com.twitter.finagle.redis.protocol.{Command, Reply, SubscribeCommand}
import com.twitter.finagle.redis.util.StringToChannelBuffer
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.Transport
import java.net.SocketAddress
import org.jboss.netty.buffer.ChannelBuffer

trait RedisRichClient { self: Client[Command, Reply] =>

  def newRichClient(dest: String): redis.Client =
     redis.Client(newService(dest))

  def newRichClient(dest: Name, label: String): redis.Client =
    redis.Client(newService(dest, label))
}

trait RichSubscribeClient { self: Client[SubscribeCommand, Unit] =>

  def newRichClient(dest: String) = redis.SubscribeClient(dest)
}

object Redis extends Client[Command, Reply] {

  object Client {
    /**
     * Default stack parameters used for redis client.
     */
    val defaultParams: Stack.Params = StackClient.defaultParams +
      param.ProtocolLibrary("redis")

    /**
     * A default client stack which supports the pipelined redis client.
     */
    def newStack: Stack[ServiceFactory[Command, Reply]] = StackClient.newStack
      .replace(DefaultPool.Role, SingletonPool.module[Command, Reply])
  }

  case class Client(
      stack: Stack[ServiceFactory[Command, Reply]] = Client.newStack,
      params: Stack.Params = Client.defaultParams)
    extends StdStackClient[Command, Reply, Client]
    with RedisRichClient {

    protected def copy1(
      stack: Stack[ServiceFactory[Command, Reply]] = this.stack,
      params: Stack.Params = this.params
    ): Client = copy(stack, params)

    protected type In = Command
    protected type Out = Reply

    protected def newTransporter(): Transporter[In, Out] =
      Netty3Transporter(redis.RedisClientPipelineFactory, params)

    protected def newDispatcher(transport: Transport[In, Out]): Service[Command, Reply] =
      new PipeliningDispatcher(transport)
  }

  val client = Client()

  def newClient(dest: Name, label: String): ServiceFactory[Command, Reply] =
    client.newClient(dest, label)

  def newService(dest: Name, label: String): Service[Command, Reply] =
    client.newService(dest, label)

  object Subscribe extends com.twitter.finagle.Client[SubscribeCommand, Unit] {

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
}