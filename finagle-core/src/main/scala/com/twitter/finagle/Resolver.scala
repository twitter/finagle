package com.twitter.finagle

import com.twitter.conversions.time._
import com.twitter.finagle.stats.{DefaultStatsReceiver, StatsReceiver}
import com.twitter.finagle.util._
import com.twitter.util._
import com.twitter.app.GlobalFlag
import com.google.common.cache.{Cache, CacheBuilder}
import java.net.{InetAddress, SocketAddress, UnknownHostException}
import java.security.{PrivilegedAction, Security}
import java.util.concurrent.TimeUnit.SECONDS
import java.util.logging.{Level, Logger}

object asyncDns extends GlobalFlag(true, "Resolve Internet addresses asynchronously.")

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
  extends NoStacktrace
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
 * [1] http://twitter.github.io/finagle/guide/Names.html
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
 * A resolver for Inet addresses.
 */
trait InetResolver extends Resolver {
  private[finagle] def bindWeightedHostPortsToAddr(hosts: Seq[InetSocketAddressUtil.WeightedHostPort]): Var[Addr]
}

/**
 * Resolver for inet scheme.
 *
 * The Var is refreshed after each TTL timeout, set from "networkaddress.cache.ttl",
 * a Java Security Property. If "networkaddress.cache.ttl" is not set or set to
 * a non-positive value, the Var is static and no future resolution is attempted.
 */
object InetResolver {
  def apply(): InetResolver = apply(DefaultStatsReceiver)
  def apply(statsReceiver: StatsReceiver): InetResolver =
    if (asyncDns()) {
      new AsyncInetResolver(statsReceiver.scope("inet").scope("dns"))
    } else {
      Logger.getLogger("InetResolver").log(Level.INFO, "Using synchronous DNS resolver")
      new SyncInetResolver
    }
}

private class AsyncInetResolver(statsReceiver: StatsReceiver) extends InetResolver {
  import InetSocketAddressUtil._

  private[this] val CACHE_SIZE = 16000L

  val scheme = "inet"
  private[this] val latencyStat = statsReceiver.stat("lookup_ms")
  private[this] val successes = statsReceiver.counter("successes")
  private[this] val failures = statsReceiver.counter("failures")
  private val log = Logger.getLogger(getClass.getName)
  private val ttlOption = {
    val t = Try(Option(java.security.AccessController.doPrivileged(
      new PrivilegedAction[String] {
        override def run(): String = Security.getProperty("networkaddress.cache.ttl")
      }
    )) map { s => s.toInt })

    t match {
      case Return(Some(value)) =>
        if (value <= 0) {
          log.log(Level.INFO,
            "networkaddress.cache.ttl is set as non-positive value, DNS cache refresh turned off")
          None
        } else {
          val duration = value.seconds
          log.log(Level.CONFIG, "networkaddress.cache.ttl found to be %s".format(duration) +
            " will refresh DNS every %s.".format(duration))
          Some(duration)
        }
      case Return(None) =>
        log.log(Level.INFO, "networkaddress.cache.ttl is not set, DNS cache refresh turned off")
        None
      case Throw(exc: NumberFormatException) =>
        log.log(Level.WARNING,
          "networkaddress.cache.ttl is set as non-number, DNS cache refresh turned off", exc)
        None
      case Throw(exc) =>
        log.log(Level.WARNING, "Unexpected Exception is thrown when getting " +
          "networkaddress.cache.ttl, DNS cache refresh turned off", exc)
        None
    }
  }
  private val timer = DefaultTimer.twitter

  private[this] val addressCacheBuilder =
    CacheBuilder.newBuilder().maximumSize(CACHE_SIZE)
  private[this] val addressCache: Cache[String, Seq[InetAddress]] = ttlOption match {
    case Some(t) => addressCacheBuilder.expireAfterWrite(t.inSeconds, SECONDS).build()
    case None => addressCacheBuilder.build()
  }

  private[finagle] def bindWeightedHostPortsToAddr(hosts: Seq[WeightedHostPort]): Var[Addr] = {
    def toAddr(whp: Seq[WeightedHostPort]): Future[Addr] = {
      val elapsed = Stopwatch.start()
      resolveWeightedHostPorts(whp, addressCache) map { addrs: Seq[SocketAddress] =>
        Addr.Bound(addrs.toSet)
      } onSuccess { _ =>
        successes.incr()
        latencyStat.add(elapsed().inMilliseconds)
      } onFailure { _ =>
        failures.incr()
      } rescue {
        case exc: UnknownHostException => Future.value(Addr.Neg: Addr)
        case NonFatal(exc) => Future.value(Addr.Failed(exc): Addr)
      }
    }

    Var.async(Addr.Pending: Addr) { u =>
      toAddr(hosts) onSuccess { u() = _ }
      ttlOption match {
        case Some(ttl) =>
          val updater = new Updater[Unit] {
            val one = Seq(())
            // Just perform one update at a time.
            protected def preprocess(elems: Seq[Unit]) = one
            protected def handle(unit: Unit) {
              // This always runs in a thread pool; it's okay to block.
              u() = Await.result(toAddr(hosts))
            }
          }
          timer.schedule(ttl.fromNow, ttl) {
            FuturePool.unboundedPool(updater())
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
      val whp = hp collect { case (host, port) =>
        (host, port, 1D)
      }
      bindWeightedHostPortsToAddr(whp)
    case Throw(exc) =>
      Var.value(Addr.Failed(exc))
  }
}

/**
 * Synchronous resolver for inet scheme. This Resolver resolves DNS after each TTL timeout.
 * The TTL is gotten from "networkaddress.cache.ttl", a Java Security Property.
 * If "networkaddress.cache.ttl" is not set or set to a non-positive value, DNS
 * cache refresh will be turned off.
 */
private class SyncInetResolver extends InetResolver {
  import InetSocketAddressUtil._

  val scheme = "inet"
  private val log = Logger.getLogger(getClass.getName)
  private val ttlOption = {
    val t = Try(Option(java.security.AccessController.doPrivileged(
      new PrivilegedAction[String] {
        override def run(): String = Security.getProperty("networkaddress.cache.ttl")
      }
    )) map { s => s.toInt })

    t match {
      case Return(Some(value)) =>
        if (value <= 0) {
          log.log(Level.INFO,
            "networkaddress.cache.ttl is set as non-positive value, DNS cache refresh turned off")
          None
        } else {
          val duration = value.seconds
          log.log(Level.CONFIG, "networkaddress.cache.ttl found to be %s".format(duration) +
            " will refresh DNS every %s.".format(duration))
          Some(duration)
        }
      case Return(None) =>
        log.log(Level.INFO, "networkaddress.cache.ttl is not set, DNS cache refresh turned off")
        None
      case Throw(exc: NumberFormatException) =>
        log.log(Level.WARNING,
          "networkaddress.cache.ttl is set as non-number, DNS cache refresh turned off", exc)
        None
      case Throw(exc) =>
        log.log(Level.WARNING, "Unexpected Exception is thrown when getting " +
          "networkaddress.cache.ttl, DNS cache refresh turned off", exc)
        None
    }
  }
  private val timer = DefaultTimer.twitter
  private val futurePool = FuturePool.unboundedPool

  /**
   * Binds to the specified hostnames, and refreshes the DNS information periodically.
   */
  def bind(hosts: String): Var[Addr] = {
    val hostPorts = parseHostPorts(hosts)
    val init = Addr.Bound(resolveHostPorts(hostPorts))
    ttlOption match {
      case Some(ttl) =>
        Var.async(init) { u =>
          val updater = new Updater[Unit] {
            val one = Seq(())
            protected def preprocess(elems: Seq[Unit]) = one
            protected def handle(unit: Unit) {
              val addr = resolveHostPorts(hostPorts)
              u() = Addr.Bound(addr)
            }
          }
          timer.schedule(ttl.fromNow, ttl) {
            futurePool {
              updater()
            } onFailure { ex =>
              log.log(Level.WARNING, "failed to resolve hosts ", ex)
            }
          }
        }
      case None =>
        Var.value(init)
    }
  }
  
  /**
   * This implementation is pretty dumb. It does not deal with TTLs. But that's ok;
   * this is used in very narrow circumstances and will anyhow be removed shortly.
   */
  private[finagle] def bindWeightedHostPortsToAddr(whp: Seq[WeightedHostPort]): Var[Addr] = {
    val (hosts, ports, weights) = whp.unzip3
    val hostports = hosts.zip(ports)
    val addrs = resolveHostPortsSeq(hostports)
    val weighted = addrs.zip(weights) collect {
      case (Seq(a, _*), w) => WeightedSocketAddress(a, w)
    }

    Var.value(Addr.Bound(weighted.toSet))
  }
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

  private[this] lazy val resolvers = {
    val rs = f()
    val log = Logger.getLogger(getClass.getName)
    val resolvers = Seq(inetResolver, NegResolver, NilResolver, FailResolver) ++ rs

    val dups = resolvers
      .groupBy(_.scheme)
      .filter { case (_, rs) => rs.size > 1 }

    if (dups.size > 0) throw new MultipleResolversPerSchemeException(dups)

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
   * simply the `addr` string, but it can be overriden by prefixing
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
   * When `name` begins with the character '/' it is intepreted to be
   * a logical name whose interpetation is subject to a
   * [[com.twitter.finagle.Dtab Dtab]].
   *
   * Eval throws exceptions upon failure to parse the name, or
   * on failure to scheme lookup. Since names are late bound,
   * binding failures are deferred.
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
   */
  private[finagle] def evalLabeled(addr: String): (Name, String) = {
    val (label, rest) = lex(addr) match {
      case El(n) :: Eq :: rest => (n, rest)
      case rest => ("", rest)
    }

    (eval(delex(rest)), label)
  }
}

object Resolver extends BaseResolver(() => LoadService[Resolver]())
