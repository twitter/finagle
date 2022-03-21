package com.twitter.finagle

import com.github.benmanes.caffeine.cache.CacheLoader
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.LoadingCache
import com.twitter.cache.EvictingCache
import com.twitter.cache.caffeine.LoadingFutureCache
import com.twitter.concurrent.AsyncSemaphore
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.InetResolver.ResolutionInterrupted
import com.twitter.finagle.stats.DefaultStatsReceiver
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.util.InetSocketAddressUtil
import com.twitter.logging.Logger
import com.twitter.util.Closable
import com.twitter.util.Var
import com.twitter.util._
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.UnknownHostException
import java.util.concurrent.atomic.AtomicInteger
import scala.util.control.NoStackTrace

private[finagle] class DnsResolver(statsReceiver: StatsReceiver, resolvePool: FuturePool)
    extends (String => Future[Seq[InetAddress]]) {

  private[this] val dnsLookupFailures = statsReceiver.counter("dns_lookup_failures")
  private[this] val dnsLookups = statsReceiver.counter("dns_lookups")
  private[this] val log = Logger()

  // Resolve hostnames asynchronously and concurrently.
  private[this] val dnsCond = new AsyncSemaphore(100)
  private[this] val waitersGauge = statsReceiver.addGauge("queue_size") { dnsCond.numWaiters }

  private[this] val Loopback = Future.value(Seq(InetAddress.getLoopbackAddress))

  override def apply(host: String): Future[Seq[InetAddress]] = {
    if (host.isEmpty || host == "localhost") {
      // Avoid using the thread pool to resolve localhost. Ideally we
      // would always do that if hostname is an IP address, but there is
      // no native API to determine if it is the case. localhost can
      // safely be treated specially here, see rfc6761 section 6.3.3.
      Loopback
    } else {
      dnsLookups.incr()
      dnsCond.acquire().flatMap { permit =>
        resolvePool(InetSocketAddressUtil.getAllByName(host).toSeq)
          .onFailure { e =>
            log.debug(s"Failed to resolve $host. Error $e")
            dnsLookupFailures.incr()
          }
          .ensure { permit.release() }
      }
    }
  }
}

/**
 * Resolver for inet scheme.
 */
object InetResolver {

  /**
   * An exception indicating that the party that requested an address resolution gave up on waiting.
   */
  object ResolutionInterrupted
      extends InterruptedException("Address resolution interrupted")
      with NoStackTrace

  def apply(): Resolver = apply(DefaultStatsReceiver)

  def apply(resolvePool: FuturePool): Resolver = apply(DefaultStatsReceiver, resolvePool)

  def apply(unscopedStatsReceiver: StatsReceiver): Resolver =
    apply(unscopedStatsReceiver, FuturePool.unboundedPool)

  def apply(unscopedStatsReceiver: StatsReceiver, resolvePool: FuturePool): Resolver =
    apply(unscopedStatsReceiver, Some(5.seconds), resolvePool)

  def apply(
    unscopedStatsReceiver: StatsReceiver,
    pollIntervalOpt: Option[Duration],
    resolvePool: FuturePool
  ) = {
    val statsReceiver = unscopedStatsReceiver.scope("inet").scope("dns")
    new InetResolver(
      new DnsResolver(statsReceiver, resolvePool),
      statsReceiver,
      pollIntervalOpt
    )
  }
}

private[finagle] class InetResolver(
  resolveHost: String => Future[Seq[InetAddress]],
  statsReceiver: StatsReceiver,
  pollIntervalOpt: Option[Duration])
    extends Resolver {
  import InetSocketAddressUtil._

  type HostPortMetadata = (String, Int, Addr.Metadata)

  val scheme = "inet"
  private[this] val latencyStat = statsReceiver.stat("lookup_ms")
  private[this] val successes = statsReceiver.counter("successes")
  private[this] val failures = statsReceiver.counter("failures")
  private[this] val cancels = statsReceiver.counter("cancels")
  private[this] val log = Logger()

  /**
   * Resolve all hostnames and merge into a final Addr.
   * If all lookups are unknown hosts, returns Addr.Neg.
   * If all lookups fail with unexpected errors, returns Addr.Failed.
   * If any lookup succeeds the final result will be Addr.Bound
   * with the successful results.
   */
  def toAddr(hp: Seq[HostPortMetadata]): Future[Addr] = {
    val elapsed = Stopwatch.start()
    Future
      .collectToTry(hp.map {
        case (host, port, meta) =>
          resolveHost(host).map { inetAddrs =>
            inetAddrs.map { inetAddr => Address.Inet(new InetSocketAddress(inetAddr, port), meta) }
          }
      })
      .flatMap { seq: Seq[Try[Seq[Address]]] =>
        // Flatten all binds and collect both successes and the first failure.
        // We do everything in one fell swoop (single collection traverse,
        // single result aggregation).
        val builder = Set.newBuilder[Address]
        var failure = Option.empty[Throwable]

        seq.foreach {
          case Return(addrs) =>
            builder ++= addrs
          case Throw(t) if failure.isEmpty =>
            failure = Some(t)
          case _ => ()
        }

        val results = builder.result()

        // Consider any result a success. Ignore partial failures.
        if (results.nonEmpty) {
          successes.incr()
          latencyStat.add(elapsed().inMilliseconds)
          Future.value(Addr.Bound(results))
        } else {
          if (failure.contains(ResolutionInterrupted)) cancels.incr()
          else failures.incr()

          latencyStat.add(elapsed().inMilliseconds)
          log.debug(s"Resolution failed (reason: $failure) for all hosts in $hp")

          failure match {
            case Some(_: UnknownHostException) => Future.value(Addr.Neg)
            case Some(e) => Future.value(Addr.Failed(e))
            case None => Future.value(Addr.Bound(Set[Address]()))
          }
        }
      }
  }

  def bindHostPortsToAddr(hosts: Seq[HostPortMetadata]): Var[Addr] = {
    Var.async(Addr.Pending: Addr) { u =>
      @volatile var closed = false

      val firstBind = toAddr(hosts).flatMap { addr =>
        u() = addr

        // Start polling if needed.
        pollIntervalOpt match {
          case Some(pollInterval) if pollInterval.isFinite =>
            def pollLoop(): Future[Unit] = {
              val start = Time.now

              toAddr(hosts).flatMap { addr =>
                u() = addr

                if (closed) Future.Done
                else {
                  val elapsed = Time.now - start
                  Future
                    .sleep(pollInterval - elapsed)(DefaultTimer)
                    .before(pollLoop())
                }
              }
            }
            pollLoop()
          case _ => Future.Done
        }
      }

      Closable.make { _ =>
        closed = true
        firstBind.raise(ResolutionInterrupted)
        Future.Done
      }
    }
  }

  /**
   * Binds to the specified hostnames, and refreshes the DNS information periodically.
   */
  def bind(hosts: String): Var[Addr] = Try(parseHostPorts(hosts)) match {
    case Return(hp) =>
      bindHostPortsToAddr(hp.map {
        case (host, port) =>
          (host, port, Addr.Metadata.empty)
      })
    case Throw(exc) =>
      Var.value(Addr.Failed(exc))
  }
}

/**
 * InetResolver that caches all successful DNS lookups indefinitely
 * and does not poll for updates.
 *
 * Clients should only use this in scenarios where host -> IP map changes
 * do not occur.
 */
object FixedInetResolver {
  private[this] val log = Logger()

  /**
   *  How many times this particular address was requested for resolution.
   */
  private final class PendingResolution extends Promise[Seq[InetAddress]] {
    val waiters: AtomicInteger = new AtomicInteger()
  }

  val scheme = "fixedinet"

  def apply(): InetResolver =
    apply(DefaultStatsReceiver)

  def apply(unscopedStatsReceiver: StatsReceiver): InetResolver =
    apply(unscopedStatsReceiver, 16000)

  def apply(unscopedStatsReceiver: StatsReceiver, maxCacheSize: Long): InetResolver =
    apply(unscopedStatsReceiver, maxCacheSize, Backoff.empty, DefaultTimer)

  /**
   * Uses a [[com.twitter.util.Future]] cache to memoize lookups.
   *
   * @param maxCacheSize Specifies the maximum number of `Futures` that can be cached.
   *                     No maximum size limit if Long.MaxValue.
   * @param backoffs Optionally retry DNS resolution failures using this sequence of
   *                 durations for backoff. [[Backoff.empty]] means don't retry.
   */
  def apply(
    unscopedStatsReceiver: StatsReceiver,
    maxCacheSize: Long,
    backoffs: Backoff,
    timer: Timer
  ): InetResolver = {
    val statsReceiver = unscopedStatsReceiver.scope("inet").scope("dns")
    new FixedInetResolver(
      cache(
        new DnsResolver(statsReceiver, FuturePool.unboundedPool),
        maxCacheSize,
        backoffs,
        timer
      ),
      statsReceiver
    )
  }

  // A size-bounded FutureCache backed by a LoaderCache
  private[finagle] def cache(
    resolveHost: String => Future[Seq[InetAddress]],
    maxCacheSize: Long,
    backoffs: Backoff = Backoff.empty,
    timer: Timer = DefaultTimer
  ): LoadingCache[String, Future[Seq[InetAddress]]] = {

    val cacheLoader = new CacheLoader[String, Future[Seq[InetAddress]]]() {
      def load(host: String): Future[Seq[InetAddress]] = {
        // Optionally retry failed DNS resolutions with specified backoff.
        def retryingLoad(nextBackoffs: Backoff): Future[Seq[InetAddress]] = {
          resolveHost(host).rescue {
            case exc: UnknownHostException =>
              if (nextBackoffs.isExhausted) {
                log.info(
                  s"Caught UnknownHostException resolving host '$host'. No more retry budget")
                Future.exception(exc)
              } else {
                log.debug(
                  s"Caught UnknownHostException resolving host '$host'. Retrying in ${nextBackoffs.duration}..."
                )
                Future.sleep(nextBackoffs.duration)(timer).before(retryingLoad(nextBackoffs.next))
              }
          }
        }

        val result = new PendingResolution
        result.become(retryingLoad(backoffs))
        result
      }
    }

    var builder = Caffeine
      .newBuilder()
      .recordStats()

    if (maxCacheSize != Long.MaxValue) {
      builder = builder.maximumSize(maxCacheSize)
    }
    builder.build(cacheLoader)
  }

  /**
   * Almost exact copy of CaffeineCache.fromLoadingCache except for it takes advantage of our own
   * (custom to InetResolver) [[PendingResolution]] promise that embeds a counter.
   */
  private def fromLoadingCache[K, V](cache: LoadingCache[K, Future[V]]): K => Future[V] = {
    val evicting = EvictingCache.lazily(new LoadingFutureCache(cache))
    new (K => Future[V]) {
      def apply(key: K): Future[V] = evicting.get(key).get match {
        case resolution: PendingResolution =>
          resolution.waiters.incrementAndGet()

          // We allow the outermost Future to be interrupted but we'd only interrupt the underlying
          // 'pending resolution' promise when no one is waiting for it to resolve (its counter is
          // at 0)
          val interruptible = Promise.attached(resolution)
          interruptible.setInterruptHandler {
            case t: Throwable =>
              if (interruptible.detach()) {
                interruptible.setException(t)
              }

              if (resolution.waiters.decrementAndGet() == 0) {
                resolution.raise(t)
              }
          }
          interruptible

        case f =>
          // Fall back to a default behavior if the underlying promise is a generic one.
          f.interruptible()
      }
    }
  }
}

/**
 * Uses a [[com.twitter.util.Future]] cache to memoize lookups.
 *
 * @param cache The lookup cache
 */
private[finagle] class FixedInetResolver(
  cache: LoadingCache[String, Future[Seq[InetAddress]]],
  statsReceiver: StatsReceiver)
    extends InetResolver(
      FixedInetResolver.fromLoadingCache(cache),
      statsReceiver,
      None
    ) {

  override val scheme = FixedInetResolver.scheme

  private[this] val cacheStatsReceiver = statsReceiver.scope("cache")
  private[this] val cacheGauges = Seq(
    cacheStatsReceiver.addGauge("size") { cache.estimatedSize },
    cacheStatsReceiver.addGauge("evicts") { cache.stats().evictionCount },
    cacheStatsReceiver.addGauge("hit_rate") { cache.stats().hitRate.toFloat }
  )
}
