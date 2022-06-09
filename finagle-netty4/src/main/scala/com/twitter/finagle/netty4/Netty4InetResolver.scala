package com.twitter.finagle.netty4

import com.twitter.finagle.Backoff
import com.twitter.finagle.FixedInetResolver
import com.twitter.finagle.InetResolver
import com.twitter.finagle.server.ServerInfo
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.FuturePool
import com.twitter.util.Promise
import com.twitter.util.Timer
import io.netty.channel.epoll.Epoll
import io.netty.channel.epoll.EpollDatagramChannel
import io.netty.channel.socket.nio.NioDatagramChannel
import io.netty.resolver.ResolvedAddressTypes
import io.netty.resolver.dns.DnsNameResolver
import io.netty.resolver.dns.DnsNameResolverBuilder
import io.netty.resolver.dns.NoopDnsCache
import io.netty.resolver.dns.NoopDnsCnameCache
import io.netty.util.concurrent.GenericFutureListener
import java.net.InetAddress
import java.util.concurrent.atomic.AtomicInteger
import io.netty.util.concurrent.{Future => NettyFuture}
import scala.collection.JavaConverters._

private object UseAsyncDnsResolver {
  private val toggle = Toggles("com.twitter.finagle.netty4.UseAsyncInetResolver")
  def apply(): Boolean = toggle(ServerInfo().id.hashCode)
}

private object AsyncDnsResolver {

  val LoopbackAddress = InetAddress.getLoopbackAddress
  val LoopbackResolverResponse = Future.value(Seq(LoopbackAddress))

  /**
   * We create one DNS resolver per JVM process for the sake of simplifying its lifecycle
   * management. We'll try shutdown the client when JVM shutdowns.
   */
  lazy val Global: DnsNameResolver = {
    val eventLoop = WorkerEventLoop.Global.next()
    val channelClass =
      if (useNativeEpoll() && Epoll.isAvailable) classOf[EpollDatagramChannel]
      else classOf[NioDatagramChannel]

    val resolver = new DnsNameResolverBuilder(eventLoop)
      .channelType(channelClass)
      // Finagle's inet resolvers have their own caching layers.
      .resolveCache(NoopDnsCache.INSTANCE)
      .cnameCache(NoopDnsCnameCache.INSTANCE)
      .resolvedAddressTypes(ResolvedAddressTypes.IPV6_PREFERRED)
      .build()

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        resolver.close()
      }
    })

    resolver
  }
}

/**
 * Adapting Netty DNS resolver to an interface that can be plugged into existing Inet resolvers.
 */
private final class AsyncDnsResolver(stats: StatsReceiver)
    extends (String => Future[Seq[InetAddress]]) {
  import AsyncDnsResolver._
  private[this] val pending = new AtomicInteger

  private[this] val dnsLookupFailures = stats.counter("dns_lookup_failures")
  private[this] val dnsLookups = stats.counter("dns_lookups")
  private[this] val pendingGauge = stats.addGauge("dns_lookups_pending")(pending.get)

  def apply(host: String): Future[Seq[InetAddress]] = host match {
    case "" =>
      LoopbackResolverResponse
    case _ if host == LoopbackAddress.getHostAddress || host == LoopbackAddress.getHostName =>
      LoopbackResolverResponse
    case _ =>
      pending.incrementAndGet()
      val result = new Promise[Seq[InetAddress]]
      val response = Global.resolveAll(host)

      response.addListener(new GenericFutureListener[NettyFuture[java.util.List[InetAddress]]] {
        def operationComplete(future: NettyFuture[java.util.List[InetAddress]]) = {
          pending.decrementAndGet()
          dnsLookups.incr()

          if (future.isSuccess()) {
            result.setValue(future.getNow.asScala.toList)
          } else {
            result.setException(future.cause())
            dnsLookupFailures.incr()
          }
        }
      })

      result.setInterruptHandler {
        // Resolution can be interrupted by Finagle. We propagate that interrupt to the underlying
        // Netty call.
        case InetResolver.ResolutionInterrupted =>
          response.cancel(false /*mayInterruptIfRunning*/ )
      }

      result
  }
}

private final class Netty4InetResolver(stats: StatsReceiver, pollIntervalOpt: Option[Duration])
    extends InetResolver(
      new AsyncDnsResolver(stats),
      stats,
      pollIntervalOpt
    )

private object Netty4InetResolver {
  final class Factory extends InetResolver.Factory {
    def apply(
      stats: StatsReceiver,
      pollIntervalOpt: Option[Duration],
      resolvePool: FuturePool
    ): InetResolver =
      if (UseAsyncDnsResolver()) new Netty4InetResolver(stats, pollIntervalOpt)
      else InetResolver.DefaultFactory(stats, pollIntervalOpt, resolvePool)
  }
}

private final class FixedNetty4InetResolver(
  stats: StatsReceiver,
  maxCacheSize: Long,
  backoff: Backoff,
  timer: Timer)
    extends FixedInetResolver(
      FixedInetResolver.cache(new AsyncDnsResolver(stats), maxCacheSize, backoff, timer),
      stats
    )

private object FixedNetty4InetResolver {
  final class Factory extends FixedInetResolver.Factory {
    def apply(
      stats: StatsReceiver,
      maxCacheSize: Long,
      backoffs: Backoff,
      timer: Timer
    ): FixedInetResolver = {
      if (UseAsyncDnsResolver()) new FixedNetty4InetResolver(stats, maxCacheSize, backoffs, timer)
      else FixedInetResolver.DefaultFactory(stats, maxCacheSize, backoffs, timer)
    }
  }
}
