package com.twitter.finagle.builder

import com.twitter.concurrent.{Spool, SpoolSource}
import com.twitter.conversions.time._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.logging.Logger
import com.twitter.util.{Duration, Future, FutureCancelledException, FuturePool, Timer}
import java.net.{InetAddress, InetSocketAddress, SocketAddress, UnknownHostException}
import java.security.Security
import scala.collection._
import scala.collection.JavaConverters._

/**
 * A dynamic cluster that keeps all DNS-resolved addresses for a host.
 *
 * Usese timer to keep itself up to date with DNS changes. Can be
 * helpful to access resources with frequently-changing addresses
 * (Akamai/Amazon S3/Google) from behind a firewall which does a
 * similar DNS lookup.
 */
protected[builder] trait DnsCluster extends Cluster[SocketAddress] {
  import Cluster._

  def ttl: Duration
  def timer: Timer
  def resolveHost: Future[Set[SocketAddress]]

  private[this] val log = Logger(this.getClass)
  private[this] var underlyingSet = Set.empty[SocketAddress]
  private[this] var changes = new SpoolSource[Cluster.Change[SocketAddress]]

  private[this] def updateAddresses(newSet: Set[SocketAddress]): Unit = synchronized {
    if (newSet != underlyingSet) {
      val added = newSet &~ underlyingSet
      val removed = underlyingSet &~ newSet

      added foreach { address =>
        changes.offer(Add(address))
      }

      removed foreach { address =>
        changes.offer(Rem(address))
      }

      underlyingSet = newSet
    }
  }

  protected[builder] def loop(): Unit = {
    resolveHost handle { case ex =>
      log.error(ex, "failed to resolve host")
      Set.empty[SocketAddress]

    } onSuccess { newSet =>
      updateAddresses(newSet)
      timer.doLater(ttl) { loop() }
    }
  }

  def snap: (Seq[SocketAddress], Future[Spool[Change[SocketAddress]]]) =
    synchronized {
      (underlyingSet.toSeq, changes())
    }
}


protected[builder] class BlockingDnsCluster(host: String, port: Int,
  val ttl: Duration, val timer: Timer = DefaultTimer.twitter) extends DnsCluster {

  private def blockingDnsCall: Set[SocketAddress] = {
    InetAddress.getAllByName(host).map { address =>
      new InetSocketAddress(address, port): SocketAddress
    }.toSet
  }

  def resolveHost: Future[Set[SocketAddress]] =
    FuturePool.unboundedPool(blockingDnsCall)

  loop()
}


object DnsCluster {

  def apply(host: String, port: Int, ttl: Duration): DnsCluster =
    new BlockingDnsCluster(host, port, ttl)

  def apply(host: String, port: Int): DnsCluster = {
    // TTL using Java standard secutity property
    val ttl = {
      val minTtl = 5.seconds
      val defaultTtl = 10.seconds
      val maxTtl = 1.hour

      val property = Option(Security.getProperty("networkaddress.cache.ttl"))
      property map (_.toInt.seconds max minTtl min maxTtl) getOrElse defaultTtl
    }

    apply(host, port, ttl)
  }
}
