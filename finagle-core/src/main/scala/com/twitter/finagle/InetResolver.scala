package com.twitter.finagle

import com.github.benmanes.caffeine.cache.CacheLoader
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.LoadingCache
import com.twitter.cache.caffeine.CaffeineCache
import com.twitter.concurrent.AsyncSemaphore
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.stats.DefaultStatsReceiver
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.util.InetSocketAddressUtil
import com.twitter.finagle.util.Updater
import com.twitter.logging.Logger
import com.twitter.util.Await
import com.twitter.util.Closable
import com.twitter.util.Var
import com.twitter.util._
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.UnknownHostException

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
      pollIntervalOpt,
      resolvePool
    )
  }
}

private[finagle] class InetResolver(
  resolveHost: String => Future[Seq[InetAddress]],
  statsReceiver: StatsReceiver,
  pollIntervalOpt: Option[Duration],
  resolvePool: FuturePool)
    extends Resolver {
  import InetSocketAddressUtil._

  type HostPortMetadata = (String, Int, Addr.Metadata)

  val scheme = "inet"
  private[this] val latencyStat = statsReceiver.stat("lookup_ms")
  private[this] val successes = statsReceiver.counter("successes")
  private[this] val failures = statsReceiver.counter("failures")
  private[this] val log = Logger()
  private[this] val timer = DefaultTimer

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
        // Filter out all successes. If there was at least 1 success, consider
        // the entire operation a success
        val results = seq.collect {
          case Return(subset) => subset
        }.flatten

        // Consider any result a success. Ignore partial failures.
        if (results.nonEmpty) {
          successes.incr()
          latencyStat.add(elapsed().inMilliseconds)
          Future.value(Addr.Bound(results.toSet))
        } else {
          // Either no hosts or resolution failed for every host
          failures.incr()
          latencyStat.add(elapsed().inMilliseconds)
          log.debug(s"Resolution failed for all hosts in $hp")

          seq.collectFirst {
            case Throw(e) => e
          } match {
            case Some(_: UnknownHostException) => Future.value(Addr.Neg)
            case Some(e) => Future.value(Addr.Failed(e))
            case None => Future.value(Addr.Bound(Set[Address]()))
          }
        }
      }
  }

  def bindHostPortsToAddr(hosts: Seq[HostPortMetadata]): Var[Addr] = {
    Var.async(Addr.Pending: Addr) { u =>
      toAddr(hosts) onSuccess { u() = _ }
      pollIntervalOpt match {
        case Some(pollInterval) =>
          val updater = new Updater[Unit] {
            val one = Seq(())
            // Just perform one update at a time.
            protected def preprocess(elems: Seq[Unit]) = one
            protected def handle(unit: Unit): Unit = {
              // This always runs in a thread pool; it's okay to block.
              u() = Await.result(toAddr(hosts))
            }
          }
          timer.schedule(pollInterval.fromNow, pollInterval) {
            resolvePool(updater(()))
          }
        case None =>
          Closable.nop
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

  val scheme = "fixedinet"

  // Temporarily cap max retries at a reasonable value until
  // we can rework this to be more sensible.
  val MaxRetries = 5

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
    originalBackoff: Backoff = Backoff.empty,
    timer: Timer = DefaultTimer
  ): LoadingCache[String, Future[Seq[InetAddress]]] = {

    // ensure backoffs is not an infinite loop. For *now* try a maximum of 5 times to
    // mitigate infinite loops seen in production. Redesign coming after the issue is mitigated
    val backoffs = originalBackoff.take(MaxRetries)

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
        retryingLoad(backoffs)
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
      CaffeineCache.fromLoadingCache(cache),
      statsReceiver,
      None,
      FuturePool.unboundedPool
    ) {

  override val scheme = FixedInetResolver.scheme

  private[this] val cacheStatsReceiver = statsReceiver.scope("cache")
  private[this] val cacheGauges = Seq(
    cacheStatsReceiver.addGauge("size") { cache.estimatedSize },
    cacheStatsReceiver.addGauge("evicts") { cache.stats().evictionCount },
    cacheStatsReceiver.addGauge("hit_rate") { cache.stats().hitRate.toFloat }
  )
}
