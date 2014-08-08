package com.twitter.finagle

import com.twitter.util.{Local, Var, Activity}
import java.io.PrintWriter
import java.net.SocketAddress
import java.util.concurrent.atomic.AtomicReference
import scala.collection.generic.CanBuildFrom
import scala.collection.immutable.VectorBuilder
import scala.collection.mutable.Builder
import scala.collection.mutable


/**
 * A Dtab--short for delegation table--comprises a sequence
 * of delegation rules. Together, these describe how to bind a
 * path to an Addr.
 */
case class Dtab(dentries0: IndexedSeq[Dentry])
    extends IndexedSeq[Dentry] with Namer {
  private lazy val dentries = dentries0.reverse

  def apply(i: Int): Dentry = dentries0(i)
  def length = dentries0.length
  override def isEmpty = length == 0

  private def lookup0(path: Path): NameTree[Path] = {
    val matches = dentries collect {
      case Dentry(prefix, dst) if path startsWith prefix =>
        val suff = path drop prefix.size
        dst map { pfx => pfx ++ suff }
    }

    if (matches.nonEmpty)
      NameTree.Alt(matches:_*)
    else
      NameTree.Neg
  }

  def lookup(path: Path): Activity[NameTree[Name]] =
    Activity.value(lookup0(path) map { path => Name(path) })

  def enum(prefix: Path): Activity[Dtab] = {
    val dtab = Dtab(dentries0 collect {
      case Dentry(path, dst) if path startsWith prefix =>
        Dentry(path drop prefix.size, dst)
    })

    Activity.value(dtab)
  }

  /**
   * Construct a Dtab representing the alternative composition of
   * ``this`` with its argument, under evaluation; that is
   *
   * {{{
   * val a, b: Dtab = ..
   * a.alt(b).lookup(path).eval == NameTree.Alt(a.lookup(path), b.lookup(path)).eval
   * }}}
   */
  def alt(other: Dtab): Dtab = other ++ this

  /**
   * Construct a Dtab representing the union composition of ``this``
   * with its argument, under evaluation; that is
   *
   * {{{
   * val a, b: Dtab = ..
   * a.union(b).lookup(path).eval == NameTree.Union(a.lookup(path), b.lookup(path)).eval
   * }}}
   */
  def union(other: Dtab): Dtab = {
    // This uses a somewhat funny but simple algorithm. We consider
    // all unique paths in the Dtabs's left-hand sides. These represent
    // all valid prefixes for the combined Dtab. We then construct a
    // new Dtab by looking up all possible prefixes in both dtabs,
    // and setting the destination name tree to their union.
    //
    // The resulting dentries each represent what a lookup in the union
    // Dtab would return for queries with this prefix. Thus, if we
    // sort the resulting Dtab by prefix size, the most specific
    // applicable dentry will be used (first); it in turn embodies
    // the most specific (and correct) answer of the combined dtab.
    //
    // Consider the union of the Dtabs
    //
    //    /a/b => /two;
    //    /a => /one
    //
    // and
    //
    //    /a/b => /three;
    //    /b => /four
    //
    // We look up, /a, /b, and /a/b, taking the union of their results; i.e.
    //
    //   /a => /one;
    //   /b => /four;
    //   /a/b => /three & (/one/b | /two)
    //
    // The Dtab
    //
    //   /foo/bar -> /quux;
    //   /foo -> /xyzzy
    //
    // unioned with itself requires looking up /foo and /foo/bar:
    //
    //   /foo => /xyzzy & /zyzzy
    //   /foo/bar => (/xyzzy/bar & /quux) | (/xyzzy/bar & /quux)

    val paths: Set[Path] = this.map(_.prefix).toSet ++ other.map(_.prefix).toSet

    val dentries = for (path: Path <- paths.toIndexedSeq) yield
      Dentry(path, NameTree.Union(this.lookup0(path), other.lookup0(path)))

    Dtab(dentries.sortBy(_.prefix.size))
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
   * @todo dedup equivalent entries so that the only the last entry is retained
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
 * Trait Dentry describes a delegation table entry.
 * It always has a prefix, describing the paths to
 * which the entry applies, and a bind method to
 * bind the given path.
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
   * where the productions ``path`` and ``tree`` are from the grammar
   * documented in [[com.twitter.finagle.NameTree$ NameTree.read]].
   */
  def read(s: String): Dentry = DentryParser(s)

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
   * Java API for ``base_=``
   */
  def setBase(dtab: Dtab) { base = dtab }

  private[this] val l = new Local[Dtab]

  /**
   * The local, or "per-request", delegation table applies to the
   * current [[com.twitter.util.Local Local]] scope which is usually
   * defined on a per-request basis. Finagle uses the Dtab
   * ``Dtab.base ++ Dtab.local`` to bind
   * [[com.twitter.finagle.Name.Path Paths]].
   *
   * Local's scope is dictated by [[com.twitter.util.Local Local]].
   *
   * The local dtab is serialized into outbound requests when
   * supported protocols are used. (Http, Thrift via TTwitter, Mux,
   * and ThriftMux are among these.) The upshot is that ``local`` is
   * defined for the entire request graph, so that a local dtab
   * defined here will apply to downstream services as well.
   */
  def local: Dtab = l() getOrElse Dtab.empty
  def local_=(dtab: Dtab) { l() = dtab }

  /**
   * Java API for ``local_=``
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
   * where the production ``dentry`` is from the grammar documented in
   * [[com.twitter.finagle.Dentry$ Dentry.read]]
   *
   */
  def read(s: String): Dtab = DtabParser(s)

  /** Scala collection plumbing required to build new dtabs */
  def newBuilder: DtabBuilder = new DtabBuilder

  implicit val canBuildFrom: CanBuildFrom[TraversableOnce[Dentry], Dentry, Dtab] =
    new CanBuildFrom[TraversableOnce[Dentry], Dentry, Dtab] {
      def apply(_ign: TraversableOnce[Dentry]): DtabBuilder = newBuilder
      def apply(): DtabBuilder = newBuilder
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

private trait DtabParsers extends NameTreePathParsers {
  lazy val dtab: Parser[Dtab] = repsep(dentry, ";") <~ opt(";") ^^ { dentries =>
    Dtab(dentries.toIndexedSeq)
  }

  lazy val dentry: Parser[Dentry] =
    path ~ "=>" ~ tree ^^ {
      case p ~ "=>" ~ t => Dentry(p, t)
    }
}

private object DtabParser extends DtabParsers {
  def apply(str: String): Dtab = synchronized {
    parseAll(dtab, str) match {
      case Success(dtab, _) => dtab
      case err: NoSuccess =>
        throw new IllegalArgumentException(err.msg+" at "+err.next.first)
    }
  }
}

private object DentryParser extends DtabParsers {
  def apply(str: String): Dentry = synchronized {
    parseAll(dentry, str) match {
      case Success(dentry, _) => dentry
      case err: NoSuccess =>
        throw new IllegalArgumentException(err.msg+" at "+err.next.first)
    }
  }
}
