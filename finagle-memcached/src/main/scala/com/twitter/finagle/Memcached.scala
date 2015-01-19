package com.twitter.finagle

import _root_.java.net.{InetSocketAddress, SocketAddress}
import com.twitter.concurrent.Broker
import com.twitter.conversions.time._
import com.twitter.finagle.cacheresolver.CacheNodeGroup
import com.twitter.finagle.client._
import com.twitter.finagle.dispatch.{SerialServerDispatcher, PipeliningDispatcher}
import com.twitter.finagle.memcached.protocol.text.{
  MemcachedClientPipelineFactory, MemcachedServerPipelineFactory}
import com.twitter.finagle.memcached.protocol.{Command, Response, RetrievalCommand, Values}
import com.twitter.finagle.memcached.{Client => MClient, Server => MServer, _}
import com.twitter.finagle.netty3._
import com.twitter.finagle.pool.SingletonPool
import com.twitter.finagle.server._
import com.twitter.finagle.stats.{ClientStatsReceiver, StatsReceiver}
import com.twitter.finagle.tracing._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.hashing.KeyHasher
import com.twitter.io.Charsets.Utf8
import com.twitter.util.{Duration, Future}
import scala.collection.mutable

private[finagle] object MemcachedTraceInitializer {
  object Module extends Stack.Module1[param.Tracer, ServiceFactory[Command, Response]] {
    val role = TraceInitializerFilter.role
    val description = "Initialize traces for the client and record hits/misses"
    def make(_tracer: param.Tracer, next: ServiceFactory[Command, Response]) = {
      val param.Tracer(tracer) = _tracer
      val filter = new Filter(tracer)
      filter andThen next
    }
  }

  class Filter(tracer: Tracer) extends SimpleFilter[Command, Response] {
    def apply(command: Command, service: Service[Command, Response]): Future[Response] =
      Trace.letTracerAndNextId(tracer) {
        Trace.recordRpc(command.name)

        val response = service(command)
        command match {
          case command: RetrievalCommand if Trace.isActivelyTracing =>
            response onSuccess {
              case Values(vals) =>
                val cmd = command.asInstanceOf[RetrievalCommand]
                val misses = mutable.Set.empty[String]
                cmd.keys foreach { key => misses += key.toString(Utf8) }
                vals foreach { value =>
                  val key = value.key.toString(Utf8)
                  Trace.recordBinary(key, "Hit")
                  misses.remove(key)
                }
                misses foreach { Trace.recordBinary(_, "Miss") }
              case _ =>
            }
          case _ =>
        }
        response
      }
    }
}

trait MemcachedRichClient { self: Client[Command, Response] =>
  def newRichClient(group: Group[SocketAddress]): memcached.Client = memcached.Client(newClient(group).toService)
  def newRichClient(group: String): memcached.Client = memcached.Client(newClient(group).toService)
  def newTwemcacheClient(group: Group[SocketAddress]) = memcached.TwemcacheClient(newClient(group).toService)
  def newTwemcacheClient(group: String) = memcached.TwemcacheClient(newClient(group).toService)
}

trait MemcachedKetamaClient {
  // TODO: make everything use varaddrs directly.

  def newKetamaClient(
    dest: String, keyHasher: KeyHasher = KeyHasher.KETAMA, ejectFailedHost: Boolean = true
  ): memcached.Client = {
    val Name.Bound(va) = Resolver.eval(dest)
    val g = Group.fromVarAddr(va)
    newKetamaClient(g, keyHasher, ejectFailedHost)
  }

  def newKetamaClient(
    group: Group[SocketAddress],
    keyHasher: KeyHasher,
    ejectFailedHost: Boolean
  ): memcached.Client = {
    new KetamaClient(
      CacheNodeGroup(group),
      keyHasher,
      KetamaClient.DefaultNumReps,
      faParams(ejectFailedHost),
      None,
      ClientStatsReceiver.scope("memcached_client")
    )
  }

  def newTwemcacheKetamaClient(
    dest: String, keyHasher: KeyHasher = KeyHasher.KETAMA, ejectFailedHost: Boolean = true
  ): memcached.TwemcacheClient = {
    val Name.Bound(va) = Resolver.eval(dest)
    val g = Group.fromVarAddr(va)
    newTwemcacheKetamaClient(g, keyHasher, ejectFailedHost)
  }

  def newTwemcacheKetamaClient(
    group: Group[SocketAddress],
    keyHasher: KeyHasher,
    ejectFailedHost: Boolean
  ): memcached.TwemcacheClient = {
    new KetamaClient(
      CacheNodeGroup(group),
      keyHasher,
      KetamaClient.DefaultNumReps,
      faParams(ejectFailedHost),
      None,
      ClientStatsReceiver.scope("twemcache_client")
    ) with TwemcachePartitionedClient
  }

  private def faParams(ejectFailedHost: Boolean) = {
    if (ejectFailedHost) MemcachedFailureAccrualClient.DefaultFailureAccrualParams
    else (Int.MaxValue, Duration.Zero)
  }
}

object MemcachedTransporter extends Netty3Transporter[Command, Response](
  "memcached", MemcachedClientPipelineFactory)

object MemcachedClient extends DefaultClient[Command, Response](
  name = "memcached",
  endpointer = Bridge[Command, Response, Command, Response](
    MemcachedTransporter, new PipeliningDispatcher(_)),
  pool = (sr: StatsReceiver) => new SingletonPool(_, sr),
  newTraceInitializer = MemcachedTraceInitializer.Module
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
  pool = (sr: StatsReceiver) => new SingletonPool(_, sr),
  failureAccrual = {
    new KetamaFailureAccrualFactory(
      _,
      numFailures     = failureAccrualParams._1,
      markDeadFor     = failureAccrualParams._2,
      timer           = DefaultTimer.twitter,
      key             = key,
      healthBroker    = broker,
      // set ejection to be true by default.
      // This is ok, since ejections is triggered only when failureAccrual
      // is enabled. With `DefaultFailureAccrualParams`, ejections will never
      // be triggered.
      ejectFailedHost = true)
  },
  newTraceInitializer = MemcachedTraceInitializer.Module
) with MemcachedRichClient

object MemcachedListener extends Netty3Listener[Response, Command](
  "memcached", MemcachedServerPipelineFactory)
object MemcachedServer extends DefaultServer[Command, Response, Response, Command](
  "memcached", MemcachedListener, new SerialServerDispatcher(_, _)
)

object Memcached extends Client[Command, Response] with MemcachedRichClient with MemcachedKetamaClient with Server[Command, Response] {
  def newClient(dest: Name, label: String): ServiceFactory[Command, Response] =
    MemcachedClient.newClient(dest, label)

  def serve(addr: SocketAddress, service: ServiceFactory[Command, Response]): ListeningServer =
    MemcachedServer.serve(addr, service)
}
