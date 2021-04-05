package com.twitter.finagle

import com.twitter.finagle.util._
import com.twitter.logging.Logger
import com.twitter.util._
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
      "Resolver not found for scheme \"%s\". Please add the jar containing this resolver to your classpath"
        .format(scheme)
    )

/**
 * Indicates that multiple [[com.twitter.finagle.Resolver Resolvers]] were
 * discovered for given `scheme`.
 *
 * Resolvers are discovered via Finagle's [[com.twitter.finagle.util.LoadService]]
 * mechanism. These exceptions typically suggest that there are multiple
 * libraries on the classpath with conflicting scheme definitions.
 */
class MultipleResolversPerSchemeException(resolvers: Map[String, Seq[Resolver]])
    extends Exception
    with NoStackTrace {
  override def getMessage = {
    val msgs = resolvers map {
      case (scheme, rs) =>
        "%s=(%s)".format(scheme, rs.map(_.getClass.getName).mkString(", "))
    } mkString (" ")
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
 * [[https://docs.oracle.com/javase/6/docs/api/java/util/ServiceLoader.html ServiceLoader]]
 * documentation for further details.
 */
trait Resolver {
  val scheme: String
  def bind(arg: String): Var[Addr]
}

/**
 * An abstract class version of Resolver for java compatibility.
 */
abstract class AbstractResolver extends Resolver

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
  private[this] val log = Logger()
  private[this] lazy val (inetResolvers, otherResolvers) = f().partition(_.scheme == "inet")
  private[this] lazy val inetResolver = {
    inetResolvers match {
      case Seq(resolver) =>
        log.info(s"Using service loaded inet resolver $resolver")
        resolver
      case Seq() =>
        log.info(s"Using default inet resolver")
        InetResolver()
      case dups =>
        log.warning(s"Multiple service loaded inet resolvers found: $dups")
        throw new MultipleResolversPerSchemeException(dups.groupBy(_.scheme))
    }
  }
  private[this] val fixedInetResolver = FixedInetResolver()

  private[this] lazy val resolvers = {
    val resolvers =
      Seq(inetResolver, fixedInetResolver, NegResolver, NilResolver, FailResolver) ++ otherResolvers

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
      case (El(s) :: ts, c) => El(s + c) :: ts
      case (ts, c) => El("" + c) :: ts
    }
  }.reverse

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
            case Some(resolver) => (resolver, delex(name))
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
