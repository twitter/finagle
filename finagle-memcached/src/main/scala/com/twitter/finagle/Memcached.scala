package com.twitter.finagle

import _root_.java.net.{InetSocketAddress, SocketAddress}
import com.twitter.concurrent.Broker
import com.twitter.conversions.time._
import com.twitter.finagle.client._
import com.twitter.finagle.dispatch.{SerialServerDispatcher, PipeliningDispatcher}
import com.twitter.finagle.memcached.protocol.text.{
  MemcachedClientPipelineFactory, MemcachedServerPipelineFactory}
import com.twitter.finagle.memcached.protocol.{Command, Response}
import com.twitter.finagle.memcached.{Client => MClient, Server => MServer, _}
import com.twitter.finagle.netty3._
import com.twitter.finagle.pool.ReusingPool
import com.twitter.finagle.server._
import com.twitter.finagle.stats.{ClientStatsReceiver, StatsReceiver}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.hashing.KeyHasher
import com.twitter.util.Duration

trait MemcachedRichClient { self: Client[Command, Response] =>
  def newRichClient(group: Group[SocketAddress]): memcached.Client = memcached.Client(newClient(group).toService)
  def newRichClient(group: String): memcached.Client = memcached.Client(newClient(group).toService)
}

trait MemcachedKetamaClient {
  def newKetamaClient(group: String, keyHasher: KeyHasher = KeyHasher.KETAMA, useFailureAccrual: Boolean = true): memcached.Client = {
    newKetamaClient(Resolver.resolve(group)(), keyHasher, useFailureAccrual)
  }

  def newKetamaClient(
      group: Group[SocketAddress],
      keyHasher: KeyHasher,
      ejectFailedHost: Boolean
  ): memcached.Client = {
    val cacheNodes = group collect {
      case node: CacheNode => node
      case addr: InetSocketAddress => new CacheNode(addr.getHostName, addr.getPort, 1)
    }

    val faParams =
      if (ejectFailedHost) MemcachedFailureAccrualClient.DefaultFailureAccrualParams
      else (Int.MaxValue, Duration.Zero)

    new KetamaClient(
      cacheNodes,
      keyHasher,
      KetamaClient.DefaultNumReps,
      faParams,
      None,
      ClientStatsReceiver.scope("memcached_client")
    )
  }
}

object MemcachedTransporter extends Netty3Transporter[Command, Response](
  "memcached", MemcachedClientPipelineFactory)

object MemcachedClient extends DefaultClient[Command, Response](
  name = "memcached",
  endpointer = Bridge[Command, Response, Command, Response](
    MemcachedTransporter, new PipeliningDispatcher(_)),
  pool = (sr: StatsReceiver) => new ReusingPool(_, sr)
) with MemcachedRichClient with MemcachedKetamaClient

private[finagle] object MemcachedFailureAccrualClient {
  val DefaultFailureAccrualParams = (5, 30.seconds)

  def apply(
      key: KetamaClientKey,
      broker: Broker[NodeHealth],
      failureAccrualParams: (Int, Duration) = DefaultFailureAccrualParams
  ): Client[Command, Response] with MemcachedRichClient = {
    new MemcachedFailureAccrualClient(key, broker, failureAccrualParams)
  }
}
private[finagle] class MemcachedFailureAccrualClient(
  key: KetamaClientKey,
  broker: Broker[NodeHealth],
  failureAccrualParams: (Int, Duration)
) extends DefaultClient[Command, Response](
  name = "memcached",
  endpointer = Bridge[Command, Response, Command, Response](
    MemcachedTransporter, new PipeliningDispatcher(_)),
  pool = (sr: StatsReceiver) => new ReusingPool(_, sr),
  failureAccrual = {
    new KetamaFailureAccrualFactory(
      _,
      failureAccrualParams._1,
      failureAccrualParams._2,
      DefaultTimer.twitter, key, broker)
  }
) with MemcachedRichClient

object MemcachedListener extends Netty3Listener[Response, Command](
  "memcached", MemcachedServerPipelineFactory)
object MemcachedServer extends DefaultServer[Command, Response, Response, Command](
  "memcached", MemcachedListener, new SerialServerDispatcher(_, _)
)

object Memcached extends Client[Command, Response] with MemcachedRichClient with MemcachedKetamaClient with Server[Command, Response] {
  def newClient(group: Group[SocketAddress]): ServiceFactory[Command, Response] =
    MemcachedClient.newClient(group)

  def serve(addr: SocketAddress, service: ServiceFactory[Command, Response]): ListeningServer =
    MemcachedServer.serve(addr, service)
}
