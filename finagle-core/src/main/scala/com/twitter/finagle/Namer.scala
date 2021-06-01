package com.twitter.finagle

import com.twitter.finagle.naming.{NameInterpreter, NamerExceededMaxDepthException, namerMaxDepth}
import com.twitter.util._
import scala.util.control.NonFatal

/**
 * A namer is a context in which a [[com.twitter.finagle.NameTree
 * NameTree]] is bound. The context is provided by the
 * [[com.twitter.finagle.Namer#lookup lookup]] method, which
 * translates [[com.twitter.finagle.Path Paths]] into
 * [[com.twitter.finagle.NameTree NameTrees]]. Namers may represent
 * external processes, for example lookups through DNS or to ZooKeeper,
 * and thus lookup results are represented by a [[com.twitter.util.Activity Activity]].
 */
abstract class Namer { self =>

  /**
   * Translate a [[com.twitter.finagle.Path Path]] into a
   * [[com.twitter.finagle.NameTree NameTree]].
   */
  def lookup(path: Path): Activity[NameTree[Name]]

  /**
   * Bind the given tree with this namer. Bind recursively follows
   * paths by looking them up in this namer. A recursion depth of up
   * to 100 is allowed.
   */
  def bind(tree: NameTree[Path]): Activity[NameTree[Name.Bound]] =
    Namer.bind(this.lookup, tree)
}

private case class FailingNamer(exc: Throwable) extends Namer {
  def lookup(path: Path): Activity[NameTree[Name]] =
    Activity.exception(exc)
}

object Namer {
  import NameTree._

  private[finagle] val namerOfKind: (String => Namer) = Memoize { kind =>
    try Class.forName(kind).newInstance().asInstanceOf[Namer]
    catch {
      case NonFatal(exc) => FailingNamer(exc)
    }
  }

  // Key to encode name tree weights in Addr metadata
  private[twitter] val AddrWeightKey = "namer_nametree_weight"

  /**
   * The global [[com.twitter.finagle.Namer Namer]]. It binds paths of the form
   *
   * {{{
   *   /$/classname/path...
   * }}}
   *
   * By reflecting in the Java class `classname` whose expected type is a
   * [[com.twitter.finagle.Namer Namer]] with a zero-arg constructor,
   * and passing the residual path to it. Lookups fail when `classname` does
   * not exist or cannot be constructed.
   *
   * The global namer also handles paths of the form
   *
   * {{{
   *   /$/nil/...
   * }}}
   *
   * to force empty resolutions.
   */
  val global: Namer = new Namer {

    private[this] object InetPath {

      private[this] def resolve(scheme: String, host: String, port: Int): Var[Addr] =
        Resolver.eval(s"$scheme!$host:$port") match {
          case Name.Bound(va) => va
          case n: Name.Path =>
            Var.value(
              Addr.Failed(new IllegalStateException(s"InetResolver returned an unbound name: $n."))
            )
        }

      def unapply(path: Path): Option[(Var[Addr], Path)] = path match {
        case Path.Utf8(
              "$",
              scheme @ ("inet" | "fixedinet"),
              host,
              IntegerString(port),
              residual @ _*) =>
          Some((resolve(scheme, host, port), Path.Utf8(residual: _*)))
        case Path.Utf8("$", scheme @ ("inet" | "fixedinet"), IntegerString(port), residual @ _*) =>
          // no host provided means localhost
          Some((resolve(scheme, "", port), Path.Utf8(residual: _*)))
        case _ => None
      }
    }

    private[this] object FailPath {
      val prefix = Path.Utf8("$", "fail")

      def unapply(path: Path): Boolean =
        path startsWith prefix
    }

    private[this] object NilPath {
      val prefix = Path.Utf8("$", "nil")

      def unapply(path: Path): Boolean =
        path startsWith prefix
    }

    private[this] object NamerPath {
      def unapply(path: Path): Option[(Namer, Path)] = path match {
        case Path.Utf8("$", kind, rest @ _*) => Some((namerOfKind(kind), Path.Utf8(rest: _*)))
        case _ => None
      }
    }

    def lookup(path: Path): Activity[NameTree[Name]] = path match {
      // Clients may depend on Name.Bound ids being Paths which resolve
      // back to the same Name.Bound.
      case InetPath(va, residual) =>
        val id = path.take(path.size - residual.size)
        Activity.value(Leaf(Name.Bound(va, id, residual)))

      case FailPath() => Activity.value(Fail)
      case NilPath() => Activity.value(Empty)
      case NamerPath(namer, rest) => namer.lookup(rest)
      case _ => Activity.value(Neg)
    }

    override def toString = "Namer.global"
  }

  /**
   * Resolve a path to an address set (taking `dtab` into account).
   */
  def resolve(dtab: Dtab, path: Path): Var[Addr] =
    NameInterpreter.bind(dtab, path).map(_.eval).run.flatMap {
      case Activity.Ok(None) => Var.value(Addr.Neg)
      case Activity.Ok(Some(names)) => Name.all(names).addr
      case Activity.Pending => Var.value(Addr.Pending)
      case Activity.Failed(exc) => Var.value(Addr.Failed(exc))
    }

  /**
   * Resolve a path to an address set (taking [[Dtab.limited]] and [[Dtab.local]] into account).
   *
   * @note The [[Path path]] resolution order will have the [[Dtab.local]] will take precedence,
   *       followed by the [[Dtab.limited]], and lastly the [[Dtab.base]]. This ensures that the
   *       [[Dtab.local]] remote request propagation behavior is retained.
   */
  def resolve(path: Path): Var[Addr] = resolve(Dtab.base ++ Dtab.limited ++ Dtab.local, path)

  /**
   * Resolve a path to an address set (taking [[Dtab.local]] into account).
   */
  def resolve(path: String): Var[Addr] =
    Try { Path.read(path) } match {
      case Return(path) => resolve(path)
      case Throw(e) => Var.value(Addr.Failed(e))
    }

  private object IntegerString {
    def unapply(s: String): Option[Int] =
      Try(s.toInt).toOption
  }

  private object DoubleString {
    def unapply(s: String): Option[Double] =
      Try(s.toDouble).toOption
  }

  /**
   * Bind the given tree by recursively following paths and looking them
   * up with the provided `lookup` function. A recursion depth of up to
   * 100 is allowed.
   */
  def bind(
    lookup: Path => Activity[NameTree[Name]],
    tree: NameTree[Path]
  ): Activity[NameTree[Name.Bound]] =
    bind(lookup, 0, None)(tree map { path => Name.Path(path) })

  private[this] def bindUnion(
    lookup: Path => Activity[NameTree[Name]],
    depth: Int,
    trees: Seq[Weighted[Name]]
  ): Activity[NameTree[Name.Bound]] = {

    val weightedTreeVars: Seq[Var[Activity.State[NameTree.Weighted[Name.Bound]]]] = trees.map {
      case Weighted(w, t) =>
        val treesAct: Activity[NameTree[Name.Bound]] = bind(lookup, depth, Some(w))(t)
        treesAct.map(Weighted(w, _)).run
    }

    val stateVar: Var[Activity.State[NameTree[Name.Bound]]] = Var.collect(weightedTreeVars).map {
      seq: Seq[Activity.State[NameTree.Weighted[Name.Bound]]] =>
        // - if there's at least one activity in Ok state, return the union of them
        // - if all activities are pending, the union is pending.
        // - if no subtree is Ok, and there are failures, retain the first failure.

        val oks = seq.collect {
          case Activity.Ok(t) => t
        }
        if (oks.isEmpty) {
          seq
            .collectFirst {
              case f @ Activity.Failed(_) => f
            }
            .getOrElse(Activity.Pending)
        } else {
          Activity.Ok(Union.fromSeq(oks).simplified)
        }
    }
    new Activity(stateVar)
  }

  // values of the returned activity are simplified and contain no Alt nodes
  private def bind(
    lookup: Path => Activity[NameTree[Name]],
    depth: Int,
    weight: Option[Double]
  )(
    tree: NameTree[Name]
  ): Activity[NameTree[Name.Bound]] =
    if (depth > namerMaxDepth())
      Activity.exception(
        new NamerExceededMaxDepthException(
          s"Max recursion level: ${namerMaxDepth()} reached in Namer lookup"))
    else
      tree match {
        case Leaf(Name.Path(path)) => lookup(path).flatMap(bind(lookup, depth + 1, weight))
        case Leaf(bound @ Name.Bound(addr)) =>
          // Add the weight of the parent to the addr's metadata
          // Note: this assumes a single level of tree weights
          val addrWithWeight = addr.map { addr =>
            (addr, weight) match {
              case (Addr.Bound(addrs, metadata), Some(weight)) =>
                Addr.Bound(addrs, metadata + ((AddrWeightKey, weight)))
              case _ => addr
            }
          }
          Activity.value(Leaf(Name.Bound(addrWithWeight, bound.id, bound.path)))

        case Fail => Activity.value(Fail)
        case Neg => Activity.value(Neg)
        case Empty => Activity.value(Empty)

        case Union() => Activity.value(Neg)
        case Union(Weighted(weight, tree)) => bind(lookup, depth, Some(weight))(tree)
        case Union(trees @ _*) => bindUnion(lookup, depth, trees)

        case Alt() => Activity.value(Neg)
        case Alt(tree) => bind(lookup, depth, weight)(tree)
        case Alt(trees @ _*) =>
          def loop(trees: Seq[NameTree[Name]]): Activity[NameTree[Name.Bound]] =
            trees match {
              case Nil => Activity.value(Neg)
              case Seq(head, tail @ _*) =>
                bind(lookup, depth, weight)(head).flatMap {
                  case Fail => Activity.value(Fail)
                  case Neg => loop(tail)
                  case head => Activity.value(head)
                }
            }
          loop(trees)
      }
}

/**
 * Abstract [[Namer]] class for Java compatibility.
 */
abstract class AbstractNamer extends Namer

/**
 * Base-trait for Namers that bind to a local Service.
 *
 * Implementers with a 0-argument constructor may be named and
 * auto-loaded with `/$/pkg.cls` syntax.
 *
 * Note that this can't actually be accomplished in a type-safe manner
 * since the naming step obscures the service's type to observers.
 */
trait ServiceNamer[Req, Rep] extends Namer {

  protected def lookupService(path: Path): Option[Service[Req, Rep]]

  def lookup(path: Path): Activity[NameTree[Name]] = lookupService(path) match {
    case None =>
      Activity.value(NameTree.Neg)
    case Some(svc) =>
      val factory = ServiceFactory(() => Future.value(svc))
      val addr = Addr.Bound(Address(factory))
      val name = Name.Bound(Var.value(addr), factory, path)
      Activity.value(NameTree.Leaf(name))
  }
}

package namer {
  final class global extends Namer {
    def lookup(path: Path) = Namer.global.lookup(path)
  }
}
