package com.twitter.finagle.redis.exp

import com.twitter.finagle.{ClientConnection, Service, ServiceFactory, Stack, Stackable}
import com.twitter.finagle.dispatch.PipeliningDispatcher
import com.twitter.finagle.param.Stats
import com.twitter.finagle.pool.SingletonPool
import com.twitter.finagle.redis.protocol.{Command, Reply}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Future, Local, Time, Timer}

object RedisPool {

  private sealed trait UseFor
  private case object Transaction extends UseFor
  private case object Subscription extends UseFor

  private val useFor = new Local[UseFor]

  def forTransaction[T](factory: ServiceFactory[Command, Reply]): Future[Service[Command, Reply]] =
    useFor.let(Transaction)(factory())

  def forSubscription[T](factory: ServiceFactory[Command, Reply])(cmd: Command): Future[Reply] =
    useFor.let(Subscription)(factory.toService(cmd))

  def newDispatcher[T](
    transport: Transport[Command, Reply],
    statsReceiver: StatsReceiver): Service[Command, Reply] =
    useFor() match {
      case Some(Subscription) => new SubscribeDispatcher(transport)
      case _                  => new PipeliningDispatcher(transport, statsReceiver, DefaultTimer.twitter)
    }

  def module: Stackable[ServiceFactory[Command, Reply]] =
    new Stack.Module1[Stats, ServiceFactory[Command, Reply]] {
      val role = Stack.Role("RedisPool")
      val description = "Manage redis connections"
      def make(_stats: Stats, next: ServiceFactory[Command, Reply]) = {
        val Stats(sr) = _stats
        new RedisPool(next, sr)
      }
    }
}

class RedisPool(
  underlying: ServiceFactory[Command, Reply],
  statsReceiver: StatsReceiver)
    extends ServiceFactory[Command, Reply] {

  private[this] val singletonPool =
    new SingletonPool(underlying, statsReceiver.scope("singletonpool"))

  private[this] val subscribePool =
    new SingletonPool(underlying, statsReceiver.scope("subscribepool"))

  final def apply(conn: ClientConnection): Future[Service[Command, Reply]] = {
    RedisPool.useFor() match {
      case Some(RedisPool.Transaction) =>
        underlying(conn)
      case Some(RedisPool.Subscription) =>
        subscribePool(conn)
      case None =>
        singletonPool(conn)
    }
  }

  final def close(deadline: Time): Future[Unit] = {
    singletonPool.close(deadline) before subscribePool.close(deadline)
  }
}
