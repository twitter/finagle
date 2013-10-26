package com.twitter.finagle

import com.twitter.finagle.util.Path
import com.twitter.util.{Local, Var}
import java.io.PrintWriter
import java.net.SocketAddress
import java.util.concurrent.atomic.AtomicReference
import scala.collection.generic.CanBuildFrom
import scala.collection.immutable.VectorBuilder
import scala.collection.mutable.Builder

/*

Note: Dtabs are currently experimental, and
are guaranteed to evolve substantially beyond
their current manifestation. Use with care.

TODO: better handling of paths. They 
need to be normalized, and probably also
tokenized throughout.

*/

/**
 * A Dtab--short for delegation table--comprises a sequence
 * of delegation rules. Together, these describe how to bind a
 * path to an Addr.
 *
 * @bug Path handling is quick and dirty. This will change.
 *
 * @define delegated
 *
 * Construct a new Dtab with the given delegation
 * entry appended.
 */
private[twitter] case class Dtab(dentries0: IndexedSeq[Dentry]) 
    extends IndexedSeq[Dentry] {
  private[this] lazy val dentries = dentries0.reverse

  def apply(i: Int): Dentry = dentries0(i)
  def length = dentries0.length

  /**
   * Bind a path in the context of this Dtab. Returns a
   * variable (observable) representation of the bound
   * address, guaranteed to be free of further delegations.
   */
  def bind(path: String): Var[Addr] = 
    bind(Path.split(path), dentries, 100)

  /**
   * Refine the name vis-à-vis this Dtab.
   *
   * @return A Name that binds with this delegation table.
   * The name is guaranteed to not be delegated in turn,
   * though it can return partially bound addresses.
   */
  def refine(name: Name): Name = 
    RefinedName(name, this, "")

  private def bind(elems: Seq[String], ents: Seq[Dentry], depth: Int): Var[Addr] = {
    if (depth == 0) Var.value(Addr.Failed("Resolution reached maximum depth"))
    else ents match {
      case Seq(d, ds@_*) if elems startsWith d.prefixElems =>
        val stripped = elems drop d.prefixElems.size
        d.bind(stripped mkString "/") flatMap {
          case Addr.Neg => 
            bind(elems, ds, depth)
          case Addr.Delegated(path) =>
            bind(Path.split(path), dentries, depth-1) flatMap {
              case Addr.Neg => bind(elems, ds, depth)
              case a => Var.value(a)
            }

          case a => Var.value(a)
        }

      case Seq(_, ds@_*) =>
        bind(elems, ds, depth)
      case Seq() =>
        Var.value(Addr.Neg)
    }
  }

  /** $delegated */
  def delegated(dentry: Dentry): Dtab =
    Dtab(dentries0 :+ dentry)

  /** $delegated */
  def delegated(prefix: String, dst: String): Dtab = 
    delegated(Dentry(prefix, dst))

  /** $delegated */
  def delegated(prefix: String, dst: Name): Dtab = 
    delegated(Dentry(prefix, dst))

  /**
   * Construct a new Dtab with the given dtab appended.
   */
  def delegated(dtab: Dtab): Dtab =
    Dtab(dentries0 ++ dtab.dentries0)

  override def toString = {
    val ds = for (Dentry(prefix, dest) <- this) yield prefix+"->"+dest.reified
    "Dtab("+(ds mkString ",")+")"
  }

  /**
   * Efficiently removes prefix `prefix` from `dtab`.
   */
  def stripPrefix(prefix: Dtab): Dtab = {
    if (this eq prefix) return Dtab.empty
    if (isEmpty) return this
    if (size < prefix.size) return this

    var i = 0
    while (i < prefix.size) {
      val d1 = this(i)
      val d2 = prefix(i)
      if (!Dentry.equiv(d1, d2))
        return this
      i += 1
    }

    if (i == size)
      Dtab.empty
    else
      Dtab(this drop prefix.size)
  }

  /**
   * Print a pretty representation of this Dtab.
   */
  def print(printer: PrintWriter) {
    printer.println("Dtab("+size+")")
    for (Dentry(prefix, dst) <- this)
      printer.println("	"+prefix+" -> "+dst.reified)
  }
}

/**
 * Trait Dentry describes a delegation table entry.
 * It always a has a prefix, describing the paths to
 * which the entry applies, and a bind method to
 * bind the given path.
 */
private[twitter] case class Dentry(prefix: String, dst: Name) {
  def bind(path: String): Var[Addr] = dst.enter(path).bind()
  val prefixElems = Path.split(prefix)
}

private[twitter] object Dentry {
  def apply(prefix: String, dst: String): Dentry =
    if (dst.startsWith("/")) Dentry(prefix, Name(dst))
    else Dentry(prefix, Resolver.eval(dst))

  /**
   * Computes functional equivalence (not necessarily 
   * object equivalence) of `d1` and `d2`.
   */
  def equiv(d1: Dentry, d2: Dentry): Boolean =
    d1.prefix == d2.prefix && d1.dst.reified == d2.dst.reified
}

private[twitter] object Dtab {
  /** Scala collection plumbing required to build new dtabs */
  def newBuilder: DtabBuilder = new DtabBuilder

  implicit val canBuildFrom: CanBuildFrom[TraversableOnce[Dentry], Dentry, Dtab] =
    new CanBuildFrom[TraversableOnce[Dentry], Dentry, Dtab] {
      def apply(_ign: TraversableOnce[Dentry]): DtabBuilder = newBuilder
      def apply(): DtabBuilder = newBuilder
    }

  private[this] val baseDtab = new AtomicReference[Dtab](null)

  /**
   * Set the base Dtab. This may only be called once, and
   * then only before the first use of Dtab.base.
   */
  def setBase(dtab: Dtab) { 
    if (!baseDtab.compareAndSet(null, dtab))
      throw new IllegalStateException("dtab is already set")
  }

  private[this] val l = new Local[Dtab]

  /**
   * Retrieve the current local dtab. When there
   * is no local Dtab, the base dtab is returned.
   */
  def apply(): Dtab = l() getOrElse base

  /**
   * Set the local dtab.
   */
  def update(dtab: Dtab) { l() = dtab }
  
  /**
   * Clear the local dtab.
   */
  def clear() { l.clear() }
  
  /**
   * Retrieve the difference between the base dtab
   * and the current local dtab.
   */
  def baseDiff(): Dtab = Dtab().stripPrefix(base)

  /**
   * Refine the given name vis-à-vis the current Dtab. The
   * refined name will not resolve to a delegated name.
   *
   * @return Refined name, whose binding is relative to the
   * current Dtab.
   */
  def refine(name: Name): Name = this().refine(name)

  val empty = Dtab(Vector.empty)

  lazy val base =  {
    if (baseDtab.get() == null)
      baseDtab.compareAndSet(null, empty)
    baseDtab.get()
  }

  def delegate(src: String, dst: String) {
    delegate(Dentry(src, dst))
  }

  def delegate(src: String, dst: Name) {
    delegate(Dentry(src, dst))
  }

  def delegate(dentry: Dentry) {
    this() = this() delegated dentry
  }
  
  def delegate(dtab: Dtab) {
    this() = this() delegated dtab
  }

  def unwind[T](f: => T): T = {
    val save = l()
    try f finally l.set(save)
  }

  /**
   * Computes functional equivalence (not necessarily 
   * object equivalence) of `d1` and `d2`.
   */
  def equiv(d1: Dtab, d2: Dtab): Boolean =
    if (d1.size != d2.size) false
    else (d1 zip d2) forall { case (e1, e2) => Dentry.equiv(e1, e2) }
}

final class DtabBuilder extends Builder[Dentry, Dtab] {
  private var builder = new VectorBuilder[Dentry]

  def +=(d: Dentry): this.type = {
    builder += d
    this
  }

  def clear() = builder.clear()

  def result(): Dtab = Dtab(builder.result)
}

private case class RefinedName(parent: Name, dtab: Dtab, suffix: String)
    extends Name {
  override def enter(path: String) = 
    RefinedName(parent, dtab, Path.join(suffix, path))

  def bind() = parent.bind() flatMap {
    case Addr.Delegated(path) => 
      dtab.bind(Path.join(path, suffix))
    case a@Addr.Bound(_) if suffix.isEmpty => Var.value(a)
    case Addr.Bound(sockaddrs) =>
      val partial: Set[SocketAddress] =
        sockaddrs map { sa => PartialSocketAddress(sa, suffix) }
      Var.value(Addr.Bound(partial))
    case a => Var.value(a)
  }
  
  val reified = "fail!"
}
