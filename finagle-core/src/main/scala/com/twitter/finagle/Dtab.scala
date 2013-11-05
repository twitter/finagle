package com.twitter.finagle

import com.twitter.finagle.util.Path
import com.twitter.util.{Local, Var}
import java.net.SocketAddress
import java.util.concurrent.atomic.AtomicReference

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
private[twitter] case class Dtab private(dentries0: Seq[Dentry]) {
  private[this] val dentries = dentries0.reverse

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
}

private[twitter] object Dtab {
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

  def unwind(f: => Unit) {
    val save = l()
    try f finally l.set(save)
  }
}

private case class RefinedName(parent: Name, dtab: Dtab, suffix: String)
    extends Name {
  override def enter(path: String) = 
    RefinedName(parent, dtab, Path.join(suffix, path))

  def bind() = parent.bind() flatMap {
    case Addr.Delegated(path) => 
      dtab.bind(Path.join(path, suffix)).memo()
    case a@Addr.Bound(_) if suffix.isEmpty => Var.value(a)
    case Addr.Bound(sockaddrs) =>
      val partial: Set[SocketAddress] =
        sockaddrs map { sa => PartialSocketAddress(sa, suffix) }
      Var.value(Addr.Bound(partial))
    case a => Var.value(a)
  }
}
