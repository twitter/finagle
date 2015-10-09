package com.twitter.finagle

import com.twitter.app.Flaggable
import com.twitter.util.Local
import java.io.PrintWriter
import scala.collection.generic.CanBuildFrom
import scala.collection.immutable.VectorBuilder
import scala.collection.mutable.Builder


/**
 * A Dtab--short for delegation table--comprises a sequence of
 * delegation rules. Together, these describe how to bind a
 * [[com.twitter.finagle.Path]] to a set of
 * [[com.twitter.finagle.Addr]]. [[com.twitter.finagle.naming.DefaultInterpreter]]
 * implements the default binding stategy.
 */
case class Dtab(dentries0: IndexedSeq[Dentry])
  extends IndexedSeq[Dentry] {

  private lazy val dentries = dentries0.reverse

  def apply(i: Int): Dentry = dentries0(i)
  def length = dentries0.length
  override def isEmpty = dentries0.isEmpty

  /**
   * Lookup the given `path` with this dtab.
   */
  def lookup(path: Path): NameTree[Name.Path] = {
    val matches = dentries.collect {
      case Dentry(prefix, dst) if path.startsWith(prefix) =>
        val suff = path.drop(prefix.size)
        dst.map { pfx => Name.Path(pfx ++ suff) }
    }

    matches.size match {
      case 0 => NameTree.Neg
      case 1 => matches.head
      case _ => NameTree.Alt(matches:_*)
    }
  }

  /**
   * Construct a new Dtab with the given delegation
   * entry appended.
   */
  def +(dentry: Dentry): Dtab =
    Dtab(dentries0 :+ dentry)

  /**
   * Java API for '+'
   */
  def append(dentry: Dentry): Dtab = this + dentry

  /**
   * Construct a new Dtab with the given dtab appended.
   */
  def ++(dtab: Dtab): Dtab = {
    if (dtab.isEmpty) this
    else Dtab(dentries0 ++ dtab.dentries0)
  }

  /**
   * Java API for '++'
   */
  def concat(dtab: Dtab): Dtab = this ++ dtab

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
      if (d1 != d2)
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
      printer.println("	"+prefix.show+" => "+dst.show)
  }

  /**
   * Simplify the Dtab. This returns a functionally equivalent Dtab
   * whose destination name trees have been simplified. The returned
   * Dtab is equivalent with respect to evaluation.
   *
   * @todo dedup equivalent entries so that only the last entry is retained
   * @todo collapse entries with common prefixes
   */
  def simplified: Dtab = Dtab({
    val simple = this map {
      case Dentry(prefix, dst) => Dentry(prefix, dst.simplified)
    }

    // Negative destinations are no-ops
    simple.filter(_.dst != NameTree.Neg)
  })

  def show: String = dentries0 map (_.show) mkString ";"
  override def toString = "Dtab("+show+")"
}

/**
 * Trait Dentry describes a delegation table entry. `prefix` describes
 * the paths that the entry applies to. `dst` describes the resulting
 * tree for this prefix on lookup.
 */
case class Dentry(prefix: Path, dst: NameTree[Path]) {
  def show = "%s=>%s".format(prefix.show, dst.show)
  override def toString = "Dentry("+show+")"
}

object Dentry {
  /**
   * Parse a Dentry from the string `s` with concrete syntax:
   * {{{
   * dentry     ::= path '=>' tree
   * }}}
   *
   * where the productions `path` and `tree` are from the grammar
   * documented in [[com.twitter.finagle.NameTree$ NameTree.read]].
   */
  def read(s: String): Dentry = NameTreeParsers.parseDentry(s)

  // The prefix to this is an illegal path in the sense that the
  // concrete syntax will not admit it. It will do for a no-op.
  val nop: Dentry = Dentry(Path.Utf8("/"), NameTree.Neg)

  implicit val equiv: Equiv[Dentry] = new Equiv[Dentry] {
    def equiv(d1: Dentry, d2: Dentry): Boolean = (
      d1.prefix == d2.prefix &&
      d1.dst.simplified == d2.dst.simplified
    )
  }
}

/**
 * Object Dtab manages 'base' and 'local' Dtabs.
 */
object Dtab {
  implicit val equiv: Equiv[Dtab] = new Equiv[Dtab] {
    def equiv(d1: Dtab, d2: Dtab): Boolean = (
      d1.size == d2.size &&
      d1.zip(d2).forall { case (de1, de2) => Equiv[Dentry].equiv(de1, de2) }
    )
  }

  /**
    * A failing delegation table.
    */
  val fail: Dtab = Dtab.read("/=>!")

  /**
   * An empty delegation table.
   */
  val empty: Dtab = Dtab(Vector.empty)

  /**
   * The base, or "system", or "global", delegation table applies to
   * every request in this process. It is generally set at process
   * startup, and not changed thereafter.
   */
  @volatile var base: Dtab = empty

  /**
   * Java API for `base_=`
   */
  def setBase(dtab: Dtab) { base = dtab }

  private[this] val l = new Local[Dtab]

  /**
   * The local, or "per-request", delegation table applies to the
   * current [[com.twitter.util.Local Local]] scope which is usually
   * defined on a per-request basis. Finagle uses the Dtab
   * `Dtab.base ++ Dtab.local` to bind [[com.twitter.finagle.Name.Path
   * Paths]] via a [[com.twitter.finagle.naming.NameInterpreter]].
   *
   * Local's scope is dictated by [[com.twitter.util.Local Local]].
   *
   * The local dtab is serialized into outbound requests when
   * supported protocols are used. (Http, Thrift via TTwitter, Mux,
   * and ThriftMux are among these.) The upshot is that `local` is
   * defined for the entire request graph, so that a local dtab
   * defined here will apply to downstream services as well.
   */
  def local: Dtab = l() match {
    case Some(dtab) => dtab
    case None => Dtab.empty
  }
  def local_=(dtab: Dtab) { l() = dtab }

  /**
   * Java API for `local_=`
   */
  def setLocal(dtab: Dtab) { local = dtab }

  def unwind[T](f: => T): T = {
    val save = l()
    try f finally l.set(save)
  }

  /**
   * Parse a Dtab from string `s` with concrete syntax
   *
   * {{{
   * dtab       ::= dentry ';' dtab | dentry
   * }}}
   *
   * where the production `dentry` is from the grammar documented in
   * [[com.twitter.finagle.Dentry$ Dentry.read]]
   *
   */
  def read(s: String): Dtab = NameTreeParsers.parseDtab(s)

  /** Scala collection plumbing required to build new dtabs */
  def newBuilder: DtabBuilder = new DtabBuilder

  implicit val canBuildFrom: CanBuildFrom[TraversableOnce[Dentry], Dentry, Dtab] =
    new CanBuildFrom[TraversableOnce[Dentry], Dentry, Dtab] {
      def apply(_ign: TraversableOnce[Dentry]): DtabBuilder = newBuilder
      def apply(): DtabBuilder = newBuilder
    }

  /**
   * implicit conversion from [[com.twitter.finagle.Dtab]] to
   * [[com.twitter.app.Flaggable]], allowing Dtabs to be easily used as
   * [[com.twitter.app.Flag]]s
   */
  implicit val flaggable: Flaggable[Dtab] = new Flaggable[Dtab] {
    override def default = None
    def parse(s: String) = Dtab.read(s)
    override def show(dtab: Dtab) = dtab.show
  }
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
