package com.twitter.finagle

import com.twitter.conversions.time._
import com.twitter.finagle.util._
import com.twitter.util.{Return, Throw, Try, Var, FuturePool}
import java.net.SocketAddress
import java.security.{PrivilegedAction, Security}
import java.util.WeakHashMap
import java.util.logging.{Level, Logger}

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
 * Resolver for inet scheme. This Resolver resolves DNS after each TTL timeout.
 * The TTL is gotten from "networkaddress.cache.ttl", a Java Security Property.
 * If "networkaddress.cache.ttl" is not set or set to a non-positive value, DNS
 * cache refresh will be turned off.
 */
object InetResolver extends Resolver {
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
    import InetSocketAddressUtil._

    val hostPorts = parseHostPorts(hosts)
    val init = Addr.Bound(resolveHostPorts(hostPorts))
    ttlOption match {
      case Some(ttl) =>
        Var.async(init) { u =>
          implicit val intPri = new Prioritized[Int] { def apply(i: Int) = i }
          val updater = Updater { _: Int =>
            val addr = resolveHostPorts(hostPorts)
            u() = Addr.Bound(addr)
          }
          timer.schedule(ttl.fromNow, ttl) {
            futurePool {
              updater(0)
            } onFailure { ex =>
              log.log(Level.WARNING, "failed to resolve hosts ", ex)
            }
          }
        }
      case None =>
        Var.value(init)
    }
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

class ResolverNotFoundException(scheme: String)
  extends Exception(
    "Resolver not found for scheme \"%s\". Please add the jar containing this resolver to your classpath".format(scheme))

class ResolverAddressInvalid(addr: String)
  extends Exception("Resolver address \"%s\" is not valid".format(addr))

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

object Resolver {
  private[this] lazy val resolvers = {
    val rs = LoadService[Resolver]()
    val log = Logger.getLogger(getClass.getName)
    val resolvers = Seq(InetResolver, NegResolver, NilResolver, FailResolver) ++ rs

    val dups = resolvers groupBy(_.scheme) filter { case (_, rs) => rs.size > 1 }
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

        case ts => (InetResolver, delex(ts))
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

private object ServerRegistry {
  private val addrNames = new WeakHashMap[SocketAddress, String]

  // This is a terrible hack until we have a better
  // way of labelling addresses.

  def register(addr: String): SocketAddress =
    addr.split("=", 2) match {
      case Array(addr) =>
        val Seq(ia) = InetSocketAddressUtil.parseHosts(addr)
        ia
      case Array(name, addr) =>
        val Seq(ia) = InetSocketAddressUtil.parseHosts(addr)
        addrNames.put(ia, name)
        ia
    }

  def nameOf(addr: SocketAddress): Option[String] =
    Option(addrNames.get(addr))
}
