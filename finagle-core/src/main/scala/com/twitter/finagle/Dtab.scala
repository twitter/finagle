package com.twitter.finagle

import com.twitter.util.{Local, Var, Activity}
import java.io.PrintWriter
import java.net.SocketAddress
import java.util.concurrent.atomic.AtomicReference
import scala.collection.generic.CanBuildFrom
import scala.collection.immutable.VectorBuilder
import scala.collection.mutable.Builder

/**
 * A Dtab--short for delegation table--comprises a sequence
 * of delegation rules. Together, these describe how to bind a
 * path to an Addr.
 */
case class Dtab(dentries0: IndexedSeq[Dentry]) 
    extends IndexedSeq[Dentry] with Namer {
  private[this] lazy val dentries = dentries0.reverse

  def apply(i: Int): Dentry = dentries0(i)
  def length = dentries0.length
  override def isEmpty = length == 0

  def lookup(path: Path): Activity[NameTree[Name]] = {
    val matches = dentries collect {
      case Dentry(prefix, dst) if path startsWith prefix =>
        val suff = path drop prefix.size
        dst map { pfx => Name(pfx ++ suff) }
    }

    if (matches.nonEmpty)
      Activity.value(NameTree.Alt(matches:_*))
    else
      Activity.value(NameTree.Neg)
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
}

/**
 * Object Dtab manages 'base' and 'local' Dtabs.
 */
object Dtab {
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
  lazy val dtab: Parser[Dtab] = repsep(dentry, ";") ^^ { dentries => 
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
