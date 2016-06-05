package com.twitter.finagle

import com.google.common.cache.{CacheLoader, CacheBuilder}
import com.twitter.cache.guava.GuavaCache
import com.twitter.concurrent.AsyncSemaphore
import com.twitter.conversions.time._
import com.twitter.finagle.stats.{DefaultStatsReceiver, StatsReceiver}
import com.twitter.finagle.util._
import com.twitter.util._
import java.net.{InetAddress, InetSocketAddress, SocketAddress, UnknownHostException}
import java.util.logging.Logger
import scala.util.control.NoStackTrace

/**
 * Indicates that a [[com.twitter.finagle.Resolver]] was not found for the
 * given `scheme`.
 *
 * Resolvers are discovered via Finagle's [[com.twitter.finagle.util.LoadService]]
 * mechanism. These exceptions typically suggest that there are no libraries
 * on the classpath that define a Resolver for the given scheme.
 */
class ResolverNotFoundException(scheme: String)
  extends Exception(
    "Resolver not found for scheme \"%s\". Please add the jar containing this resolver to your classpath".format(scheme))

/**
 * Indicates that multiple [[com.twitter.finagle.Resolver Resolvers]] were
 * discovered for given `scheme`.
 *
 * Resolvers are discovered via Finagle's [[com.twitter.finagle.util.LoadService]]
 * mechanism. These exceptions typically suggest that there are multiple
 * libraries on the classpath with conflicting scheme definitions.
 */
class MultipleResolversPerSchemeException(resolvers: Map[String, Seq[Resolver]])
  extends Exception with NoStackTrace
{
  override def getMessage = {
    val msgs = resolvers map { case (scheme, rs) =>
      "%s=(%s)".format(scheme, rs.map(_.getClass.getName).mkString(", "))
    } mkString(" ")
    "Multiple resolvers defined: %s".format(msgs)
  }
}

/**
 * Indicates that a destination name string passed to a
 * [[com.twitter.finagle.Resolver]] was invalid according to the destination
 * name grammar [1].
 *
 * [1] https://twitter.github.io/finagle/guide/Names.html
 */
class ResolverAddressInvalid(addr: String)
  extends Exception("Resolver address \"%s\" is not valid".format(addr))

/**
 * A resolver binds a name, represented by a string, to a
 * variable address. Resolvers have an associated scheme
 * which is used for lookup so that names may be resolved
 * in a global context.
 *
 * These are loaded by Finagle through the
 * [[com.twitter.finagle.util.LoadService service loading mechanism]]. Thus, in
 * order to implement a new resolver, a class implementing `Resolver` with a
 * 0-arg constructor must be registered in a file named
 * `META-INF/services/com.twitter.finagle.Resolver` included in the classpath; see
 * Oracle's
 * [[http://docs.oracle.com/javase/6/docs/api/java/util/ServiceLoader.html ServiceLoader]]
 * documentation for further details.
 */
trait Resolver {
  val scheme: String
  def bind(arg: String): Var[Addr]

  @deprecated("Use Resolver.bind", "6.7.x")
  final def resolve(name: String): Try[Group[SocketAddress]] =
    bind(name) match {
      case Var.Sampled(Addr.Failed(e)) => Throw(e)
      case va => Return(Group.fromVarAddr(va))
    }
}

/**
 * An abstract class version of Resolver for java compatibility.
 */
abstract class AbstractResolver extends Resolver

/**
 * Resolver for inet scheme.
 */
object InetResolver {
  def apply(): Resolver = apply(DefaultStatsReceiver)
  def apply(statsReceiver: StatsReceiver): Resolver =
    new InetResolver(statsReceiver, Some(5.seconds))
}

private[finagle] class InetResolver(
  unscopedStatsReceiver: StatsReceiver,
  pollIntervalOpt: Option[Duration]
) extends Resolver {
  import InetSocketAddressUtil._

  type HostPortMetadata = (String, Int, Addr.Metadata)

  val scheme = "inet"
  protected[this] val statsReceiver = unscopedStatsReceiver.scope("inet").scope("dns")
  private[this] val latencyStat = statsReceiver.stat("lookup_ms")
  private[this] val successes = statsReceiver.counter("successes")
  private[this] val failures = statsReceiver.counter("failures")
  private[this] val dnsLookupFailures = statsReceiver.counter("dns_lookup_failures")
  private[this] val dnsLookups = statsReceiver.counter("dns_lookups")
  private val log = Logger.getLogger(getClass.getName)
  private val timer = DefaultTimer.twitter

  /*
   * Resolve hostnames asynchronously and concurrently.
   */
  private[this] val dnsCond = new AsyncSemaphore(100)
  private val waitersGauge = statsReceiver.addGauge("queue_size") { dnsCond.numWaiters }
  protected def resolveHost(host: String): Future[Seq[InetAddress]] = {
    dnsLookups.incr()
    dnsCond.acquire().flatMap { permit =>
      FuturePool.unboundedPool(InetAddress.getAllByName(host).toSeq)
        .onFailure{ e =>
          log.warning(s"Failed to resolve $host. Error $e")
          dnsLookupFailures.incr()
        }
        .ensure { permit.release() }
    }
  }

  /**
    * Resolve all hostnames and merge into a final Addr.
    * If all lookups are unknown hosts, returns Addr.Neg.
    * If all lookups fail with unexpected errors, returns Addr.Failed.
    * If any lookup succeeds the final result will be Addr.Bound
    * with the successful results.
    */
  def toAddr(hp: Seq[HostPortMetadata]): Future[Addr] = {
    val elapsed = Stopwatch.start()
    Future.collectToTry(hp.map {
      case (host, port, meta) =>
        resolveHost(host).map { inetAddrs =>
          inetAddrs.map { inetAddr =>
            Address.Inet(new InetSocketAddress(inetAddr, port), meta)
          }
        }
    }).flatMap { seq: Seq[Try[Seq[Address]]] =>
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
        log.warning("Resolution failed for all hosts")

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
            protected def handle(unit: Unit) {
              // This always runs in a thread pool; it's okay to block.
              u() = Await.result(toAddr(hosts))
            }
          }
          timer.schedule(pollInterval.fromNow, pollInterval) {
            FuturePool.unboundedPool(updater(()))
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
      bindHostPortsToAddr(hp.map { case (host, port) =>
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

  val scheme = "fixedinet"

  def apply(): InetResolver =
    apply(DefaultStatsReceiver)
  
  def apply(statsReceiver: StatsReceiver): InetResolver =
    apply(statsReceiver, 16000)
  
  def apply(statsReceiver: StatsReceiver, maxCacheSize: Long): InetResolver =
    new FixedInetResolver(statsReceiver, maxCacheSize, None)
}

/**
 * Uses a [[com.twitter.util.Future]] cache to memoize lookups.
 *
 * Allows unit tests to specify a CI-friendly resolve fn. Otherwise
 * defaults to InetResolver.resolveHost
 *
 * @param maxCacheSize Specifies the maximum number of `Futures` that can be cached.
 *                     No maximum size limit if Long.MaxValue.
 * @param resolveOverride Optional fn. If None, defaults back to superclass implementation
 */
private[finagle] class FixedInetResolver(
    unscopedStatsReceiver: StatsReceiver,
    maxCacheSize: Long,
    resolveOverride: Option[String => Future[Seq[InetAddress]]]
  ) extends InetResolver(unscopedStatsReceiver, None) {

  override val scheme = FixedInetResolver.scheme

  // fallback to InetResolver.resolveHost if no override was provided
  val resolveFn: (String => Future[Seq[InetAddress]]) =
    resolveOverride.getOrElse(super.resolveHost)

  // A size-bounded FutureCache backed by a LoaderCache
  private[this] val cache = {
    var builder = CacheBuilder
      .newBuilder()
      .recordStats()

    if (maxCacheSize != Long.MaxValue) {
      builder = builder.maximumSize(maxCacheSize)
    }

    builder
      .build(
        new CacheLoader[String, Future[Seq[InetAddress]]]() {
          def load(host: String) = resolveFn(host)
        })
  }

  private[this] val cacheStatsReceiver = statsReceiver.scope("cache")
  private[this] val cacheGauges = Seq(
    cacheStatsReceiver.addGauge("size") { cache.size },
    cacheStatsReceiver.addGauge("evicts") { cache.stats().evictionCount },
    cacheStatsReceiver.addGauge("hit_rate") { cache.stats().hitRate.toFloat })

  private[this] val futureCache = GuavaCache.fromLoadingCache(cache)

  override def resolveHost(host: String): Future[Seq[InetAddress]] =
    futureCache(host)
}

object NegResolver extends Resolver {
  val scheme = "neg"
  def bind(arg: String) = Var.value(Addr.Neg)
}

object NilResolver extends Resolver {
  val scheme = "nil"
  def bind(arg: String) = Var.value(Addr.Bound())
}

object FailResolver extends Resolver {
  val scheme = "fail"
  def bind(arg: String) = Var.value(Addr.Failed(new Exception(arg)))
}

private[finagle] abstract class BaseResolver(f: () => Seq[Resolver]) {
  private[this] val inetResolver = InetResolver()
  private[this] val fixedInetResolver = FixedInetResolver()

  private[this] lazy val resolvers = {
    val rs = f()
    val log = Logger.getLogger(getClass.getName)
    val resolvers = Seq(inetResolver, fixedInetResolver, NegResolver, NilResolver, FailResolver) ++ rs

    val dups = resolvers
      .groupBy(_.scheme)
      .filter { case (_, rs) => rs.size > 1 }

    if (dups.nonEmpty) throw new MultipleResolversPerSchemeException(dups)

    for (r <- resolvers)
      log.info("Resolver[%s] = %s(%s)".format(r.scheme, r.getClass.getName, r))

    resolvers
  }

  def get[T <: Resolver](clazz: Class[T]): Option[T] =
    resolvers find { _.getClass isAssignableFrom clazz } map { _.asInstanceOf[T] }

  private[this] sealed trait Token
  private[this] case class El(e: String) extends Token
  private[this] object Eq extends Token
  private[this] object Bang extends Token

  private[this] def delex(ts: Seq[Token]) =
    ts map {
      case El(e) => e
      case Bang => "!"
      case Eq => "="
    } mkString ""

  private[this] def lex(s: String) = {
    s.foldLeft(List[Token]()) {
      case (ts, '=') => Eq :: ts
      case (ts, '!') => Bang :: ts
      case (El(s) :: ts, c) => El(s+c) :: ts
      case (ts, c) => El(""+c) :: ts
    }
  }.reverse

  /**
   * Resolve a group from an address, a string. Resolve uses
   * `Resolver`s to do this. These are loaded via the Java
   * [[http://docs.oracle.com/javase/6/docs/api/java/util/ServiceLoader.html ServiceLoader]]
   * mechanism. The default resolver is "inet", resolving DNS
   * name/port pairs.
   *
   * Target names have a simple grammar: The name of the resolver
   * precedes the name of the address to be resolved, separated by
   * an exclamation mark ("bang"). For example: inet!twitter.com:80
   * resolves the name "twitter.com:80" using the "inet" resolver. If no
   * resolver name is present, the inet resolver is used.
   *
   * Names resolved by this mechanism are also a
   * [[com.twitter.finagle.LabelledGroup]]. By default, this name is
   * simply the `addr` string, but it can be overridden by prefixing
   * a name separated by an equals sign from the rest of the addr.
   * For example, the addr "www=inet!google.com:80" resolves
   * "google.com:80" with the inet resolver, but the returned group's
   * [[com.twitter.finagle.LabelledGroup]] name is "www".
   */
  @deprecated("Use Resolver.eval", "6.7.x")
  def resolve(addr: String): Try[Group[SocketAddress]] =
    Try { eval(addr) } flatMap {
      case Name.Path(_) =>
        Throw(new IllegalArgumentException("Resolver.resolve does not support logical names"))
      case bound@Name.Bound(_) =>
        Return(NameGroup(bound))
    }

  /**
   * Parse and evaluate the argument into a Name. Eval parses
   * a simple grammar: a scheme is followed by a bang, followed
   * by an argument:
   * 	name := scheme ! arg
   * The scheme is looked up from registered Resolvers, and the
   * argument is passed in.
   *
   * When `name` begins with the character '/' it is interpreted to be
   * a logical name whose interpretation is subject to a
   * [[com.twitter.finagle.Dtab Dtab]].
   *
   * Eval throws exceptions upon failure to parse the name, or
   * on failure to scheme lookup. Since names are late bound,
   * binding failures are deferred.
   *
   * @see [[Resolvers.eval]] for Java support
   */
  def eval(name: String): Name =
    if (name startsWith "/") Name(name)
    else {
      val (resolver, arg) = lex(name) match {
        case (Eq :: _) | (Bang :: _) =>
          throw new ResolverAddressInvalid(name)

        case El(scheme) :: Bang :: name =>
          resolvers.find(_.scheme == scheme) match {
            case Some(resolver) =>  (resolver, delex(name))
            case None => throw new ResolverNotFoundException(scheme)
          }

        case ts => (inetResolver, delex(ts))
      }

      Name.Bound(resolver.bind(arg), name)
    }

  /**
   * Parse and evaluate the argument into a (Name, label: String) tuple.
   * Arguments are parsed with the same grammar as in `eval`. If a label is not
   * provided (i.e. no "label=<addr>"), then the empty string is returned.
   *
   * @see [[Resolvers.evalLabeled]] for Java support
   */
  def evalLabeled(addr: String): (Name, String) = {
    val (label, rest) = lex(addr) match {
      case El(n) :: Eq :: rest => (n, rest)
      case rest => ("", rest)
    }

    (eval(delex(rest)), label)
  }
}

/**
 * The default [[Resolver]] used by Finagle.
 *
 * @see [[Resolvers]] for Java support.
 */
object Resolver extends BaseResolver(() => LoadService[Resolver]())

/**
 * Java APIs for [[Resolver]].
 */
object Resolvers {

  /**
   * @see [[Resolver.eval]]
   */
  def eval(name: String): Name =
    Resolver.eval(name)

  /**
   * @see [[Resolver.evalLabeled]]
   */
  def evalLabeled(addr: String): (Name, String) =
    Resolver.evalLabeled(addr)

}
