package com.twitter.finagle

import _root_.java.net.{InetSocketAddress, SocketAddress}
import com.twitter.concurrent.Broker
import com.twitter.conversions.time._
import com.twitter.finagle.cacheresolver.CacheNodeGroup
import com.twitter.finagle.client._
import com.twitter.finagle.dispatch.{SerialServerDispatcher, PipeliningDispatcher}
import com.twitter.finagle.memcachedx.protocol.text.{
  MemcachedClientPipelineFactory, MemcachedServerPipelineFactory}
import com.twitter.finagle.memcachedx.protocol.{Command, Response, RetrievalCommand, Values}
import com.twitter.finagle.memcachedx.{Client => MClient, Server => MServer, _}
import com.twitter.finagle.netty3._
import com.twitter.finagle.pool.SingletonPool
import com.twitter.finagle.server._
import com.twitter.finagle.stats.{ClientStatsReceiver, StatsReceiver}
import com.twitter.finagle.tracing._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.hashing.KeyHasher
import com.twitter.io.Buf
import com.twitter.util.{Duration, Future}
import scala.collection.mutable

private[finagle] object MemcachedxTraceInitializer {
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
        val response = service(command)
        Trace.recordRpc(command.name)
        command match {
          case command: RetrievalCommand if Trace.isActivelyTracing =>
            response onSuccess {
              case Values(vals) =>
                val cmd = command.asInstanceOf[RetrievalCommand]
                val misses = mutable.Set.empty[String]
                cmd.keys foreach { case Buf.Utf8(key) => misses += key }
                vals foreach { value =>
                  val Buf.Utf8(key) = value.key
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

trait MemcachedxRichClient { self: Client[Command, Response] =>
  def newRichClient(group: Group[SocketAddress]): memcachedx.Client = memcachedx.Client(newClient(group).toService)
  def newRichClient(group: String): memcachedx.Client = memcachedx.Client(newClient(group).toService)
  def newTwemcacheClient(group: Group[SocketAddress]) = memcachedx.TwemcacheClient(newClient(group).toService)
  def newTwemcacheClient(group: String) = memcachedx.TwemcacheClient(newClient(group).toService)
}

trait MemcachedxKetamaClient {
  // TODO: make everything use varaddrs directly.

  def newKetamaClient(
    dest: String, keyHasher: KeyHasher = KeyHasher.KETAMA, ejectFailedHost: Boolean = true
  ): memcachedx.Client = {
    val Name.Bound(va) = Resolver.eval(dest)
    val g = Group.fromVarAddr(va)
    newKetamaClient(g, keyHasher, ejectFailedHost)
  }

  def newKetamaClient(
    group: Group[SocketAddress],
    keyHasher: KeyHasher,
    ejectFailedHost: Boolean
  ): memcachedx.Client = {
    new KetamaClient(
      initialServices       = CacheNodeGroup(group),
      keyHasher             = keyHasher,
      numReps               = KetamaClient.DefaultNumReps,
      failureAccrualParams  = faParams(ejectFailedHost),
      legacyFAClientBuilder = None,
      statsReceiver         = ClientStatsReceiver.scope("memcached_client")
    )
  }

  def newTwemcacheKetamaClient(
    dest: String, keyHasher: KeyHasher = KeyHasher.KETAMA, ejectFailedHost: Boolean = true
  ): memcachedx.TwemcacheClient = {
    val Name.Bound(va) = Resolver.eval(dest)
    val g = Group.fromVarAddr(va)
    newTwemcacheKetamaClient(g, keyHasher, ejectFailedHost)
  }

  def newTwemcacheKetamaClient(
    group: Group[SocketAddress],
    keyHasher: KeyHasher,
    ejectFailedHost: Boolean
  ): memcachedx.TwemcacheClient = {
    new KetamaClient(
      initialServices       = CacheNodeGroup(group),
      keyHasher             = keyHasher,
      numReps               = KetamaClient.DefaultNumReps,
      failureAccrualParams  = faParams(ejectFailedHost),
      legacyFAClientBuilder = None,
      statsReceiver         = ClientStatsReceiver.scope("twemcache_client")
    ) with TwemcachePartitionedClient
  }

  private def faParams(ejectFailedHost: Boolean) = {
    if (ejectFailedHost) MemcachedxFailureAccrualClient.DefaultFailureAccrualParams
    else (Int.MaxValue, () => Duration.Zero)
  }
}

object MemcachedxTransporter extends Netty3Transporter[Command, Response](
  "memcached", MemcachedClientPipelineFactory)

// deprecated, in favor of `c.t.f.memcached.Memcached`, 2015-02-22
object MemcachedxClient extends DefaultClient[Command, Response](
  name = "memcached",
  endpointer = Bridge[Command, Response, Command, Response](
    MemcachedxTransporter, new PipeliningDispatcher(_)),
  pool = (sr: StatsReceiver) => new SingletonPool(_, sr),
  newTraceInitializer = MemcachedxTraceInitializer.Module
) with MemcachedxRichClient with MemcachedxKetamaClient

private[finagle] object MemcachedxFailureAccrualClient {
  val DefaultFailureAccrualParams = (5, () => 30.seconds)

  def apply(
      key: KetamaClientKey,
      broker: Broker[NodeHealth],
      failureAccrualParams: (Int, () => Duration) = DefaultFailureAccrualParams
  ): Client[Command, Response] with MemcachedxRichClient = {
    new MemcachedxFailureAccrualClient(key, broker, failureAccrualParams)
  }
}
private[finagle] class MemcachedxFailureAccrualClient(
  key: KetamaClientKey,
  broker: Broker[NodeHealth],
  failureAccrualParams: (Int, () => Duration),
    statsReceiver: StatsReceiver = ClientStatsReceiver
) extends DefaultClient[Command, Response](
  name = "memcached",
  endpointer = Bridge[Command, Response, Command, Response](
    MemcachedxTransporter, new PipeliningDispatcher(_)),
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
      ejectFailedHost = true,
      statsReceiver   = statsReceiver)
  },
  newTraceInitializer = MemcachedxTraceInitializer.Module
) with MemcachedxRichClient

object MemcachedxListener extends Netty3Listener[Response, Command](
  "memcached", MemcachedServerPipelineFactory)
object MemcachedxServer extends DefaultServer[Command, Response, Response, Command](
  "memcached", MemcachedxListener, new SerialServerDispatcher(_, _)
)

object Memcachedx extends Client[Command, Response] with MemcachedxRichClient with MemcachedxKetamaClient with Server[Command, Response] {
  def newClient(dest: Name, label: String): ServiceFactory[Command, Response] =
    MemcachedxClient.newClient(dest, label)
    
  def newService(dest: Name, label: String): Service[Command, Response] =
    MemcachedxClient.newService(dest, label)

  def serve(addr: SocketAddress, service: ServiceFactory[Command, Response]): ListeningServer =
    MemcachedxServer.serve(addr, service)
}
