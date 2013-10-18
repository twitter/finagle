package com.twitter.finagle.builder

import com.google.common.collect.HashMultiset
import com.twitter.concurrent.Spool
import com.twitter.conversions.time._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.logging.Logger
import com.twitter.util.{Duration, Future, FutureCancelledException, FuturePool, Promise, Return, Timer}
import java.net.{InetAddress, InetSocketAddress, SocketAddress, UnknownHostException}
import java.security.Security
import scala.collection._
import scala.collection.JavaConverters._

/** A dynamic cluster that keeps all DNS-resolved addresses for a
 * host.
 *
 * Runs a timer to keep itself up to date with DNS changes. Can be
 * helpful to access resources with frequently-changing addresses
 * (Akamai/Amazon S3/Google) from behind a firewall which does a
 * similar DNS lookup.
 */
class DnsCluster(host: String, port: Int, ttl: Duration,
                 timer: Timer = DefaultTimer.twitter)
  extends Cluster[SocketAddress] {

  private[this] val log = Logger(this.getClass)

  private[this] var underlyingSet = Set.empty[SocketAddress]
  private[this] var changes = new Promise[Spool[Cluster.Change[SocketAddress]]]

  // exposed for testing
  protected[builder] def blockingDnsCall: Set[SocketAddress] = {
    InetAddress.getAllByName(host).map { address =>
      new InetSocketAddress(address, port): SocketAddress
    }.toSet
  }

  // exposed for testing
  protected[builder] def resolveHost: Future[Set[SocketAddress]] =
    FuturePool.unboundedPool(blockingDnsCall) handle {
      case ex: UnknownHostException =>
        log.error("DNS request failed for host %s", host)
        Set.empty[SocketAddress]
      case ex =>
        log.error(ex, "failed to update %s", host)
        underlyingSet
    }

  private[this] def appendUpdate(update: Cluster.Change[SocketAddress]) = {
    val newTail = new Promise[Spool[Cluster.Change[SocketAddress]]]
    changes() = Return(update *:: newTail)
    changes = newTail
  }

  private[this] def updateAddresses(newSet: Set[SocketAddress]): Unit = synchronized {
    if (newSet != underlyingSet) {
      val added = newSet &~ underlyingSet
      val removed = underlyingSet &~ newSet

      added foreach { address =>
        appendUpdate(Cluster.Add(address))
      }

      removed foreach { address =>
        appendUpdate(Cluster.Rem(address))
      }

      underlyingSet = newSet

      log.debug("%s is now %s", host, underlyingSet)
    }
  }

  // exectutes the future after a delay.. is there a standard function for this
  def scheduleFuture[T](d: Duration)(f: => Future[T]): Future[T] =
    timer.doLater(d)(()) flatMap { _ => f }

  private[this] def loop(): Future[Unit] =
    resolveHost flatMap { newSet =>
      updateAddresses(newSet)
      scheduleFuture(ttl) { loop() }
    }

  log.debug("starting DNS cluster for %s:%d, ttl %s", host, port, ttl)

  // scheduleFuture(0) here helps dealing with test subclasses that
  // override blockingDnsCall or resolveHost, calling loop() directly
  // from constructor would cause an NPE in tests
  val loopyFuture = scheduleFuture(0.millis) { loop() }

  def stop(): Unit = loopyFuture.raise(new FutureCancelledException)

  def snap: (Seq[SocketAddress], Future[Spool[Cluster.Change[SocketAddress]]]) =
    synchronized {
      (underlyingSet.toSeq, changes)
    }
}


object DnsCluster {

  def apply(host: String, port: Int): DnsCluster = {

    // TTL using Java secutity property
    // http://stackoverflow.com/questions/1256556/any-way-to-make-java-honor-the-dns-caching-timeout-ttl
    val ttl = {
      val minTtl = 5.seconds
      val defaultTtl = 10.seconds
      val maxTtl = 1.hour

      val property = Option(Security.getProperty("networkaddress.cache.ttl"))
      property map (_.toInt.seconds max minTtl min maxTtl) getOrElse defaultTtl
    }

    new DnsCluster(host, port, ttl)
  }
}
