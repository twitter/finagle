package com.twitter.finagle

import com.twitter.finagle.util.{InetSocketAddressUtil, LoadService}
import com.twitter.util.{Closable, Return, Throw, Time, Try}
import java.net.SocketAddress
import java.util.WeakHashMap
import java.util.logging.Logger
import scala.collection.mutable

trait ResolvedGroup extends Group[SocketAddress]
  with Proxy
  with NamedGroup
{
  val addrs: List[String]
}

trait Resolver {
  val scheme: String
  def resolve(name: String): Try[Group[SocketAddress]]
}

object InetResolver extends Resolver {
  val scheme = "inet"

  def resolve(addr: String) = {
    val expanded = InetSocketAddressUtil.parseHosts(addr)
    Return(Group[SocketAddress](expanded:_*))
  }
}

class ResolverNotFoundException(scheme: String)
  extends Exception("Resolver not found for scheme \"%s\"".format(scheme))

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
    val resolvers = Seq(InetResolver) ++ rs

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

  private[this] val _resolutions = mutable.Set.empty[List[String]]
  def resolutions = synchronized { _resolutions.toSet }

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
   * [[com.twitter.finagle.NamedGroup]]. By default, this name is
   * simply the `addr` string, but it can be overriden by prefixing
   * a name separated by an equals sign from the rest of the addr.
   * For example, the addr "www=inet!google.com:80" resolves
   * "google.com:80" with the inet resolver, but the returned group's
   * [[com.twitter.finagle.NamedGroup]] name is "www".
   */
  def resolve(addr: String): Try[Group[SocketAddress]] = {
    val lexed = lex(addr)

    val (groupName, stripped) = lexed match {
      case El(n) :: Eq :: rest => (n, rest)
      case Eq :: rest => ("", rest)
      case rest => (addr, rest)
    }

    val resolved = stripped match {
      case (Eq :: _) | (Bang :: _) =>
        Throw(new ResolverAddressInvalid(addr))

      case El(scheme) :: Bang :: addr =>
        resolvers.find(_.scheme == scheme) match {
          case Some(resolver) => resolver.resolve(delex(addr))
          case None => Throw(new ResolverNotFoundException(scheme))
        }

      case ts =>
        InetResolver.resolve(delex(ts))
    }

    resolved map { group =>
      val lastAddrs = group match {
        case g: ResolvedGroup => g.addrs
        case g => Nil
      }

      val resolvedGroup = new ResolvedGroup {
        val addrs = delex(stripped) :: lastAddrs
        val name = groupName
        val self = group
        def members = self.members
      }

      synchronized {
        _resolutions -= lastAddrs
        _resolutions += resolvedGroup.addrs
      }

      resolvedGroup
    }
  }
}

private object ServerRegistry {
  private val addrNames = new WeakHashMap[SocketAddress, String]

  // This is a terrible hack until we have a better
  // way of naming addresses.

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
