package com.twitter.finagle

import com.twitter.app.Flaggable
import com.twitter.io.Buf
import com.twitter.util.Local
import java.io.PrintWriter

private[finagle] trait DtabBase extends IndexedSeq[Dentry] { self: Dtab =>

  val dentries0: IndexedSeq[Dentry]
  private lazy val dentries = dentries0.reverse

  def apply(i: Int): Dentry = dentries0(i)
  def length: Int = dentries0.length
  override def isEmpty: Boolean = dentries0.isEmpty

  /**
   * Lookup the given `path` with this dtab.
   */
  def lookup(path: Path): NameTree[Name.Path] = {
    val matches = dentries.collect {
      case Dentry(prefix, dst) if prefix.matches(path) =>
        val suff = path.drop(prefix.size)
        dst.map { pfx => Name.Path(pfx ++ suff) }
    }

    matches.size match {
      case 0 => NameTree.Neg
      case 1 => matches.head
      case _ => NameTree.Alt(matches: _*)
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
  def print(printer: PrintWriter): Unit = {
    printer.println("Dtab(" + size + ")")
    for (Dentry(prefix, dst) <- this)
      printer.println("	" + prefix.show + " => " + dst.show)
  }

  /**
   * Simplify the Dtab. This returns a functionally equivalent Dtab
   * whose destination name trees have been simplified. The returned
   * Dtab is equivalent with respect to evaluation.
   *
   * @todo dedup equivalent entries so that only the last entry is retained
   * @todo collapse entries with common prefixes
   */
  def simplified: Dtab =
    Dtab({
      val simple = self.map((entry: Dentry) => entry.copy(dst = entry.dst.simplified))
      // Negative destinations are no-ops
      simple.filter(_.dst != NameTree.Neg)
    })

  def show: String = dentries0 map (_.show) mkString ";"
  override def toString: String = "Dtab(" + show + ")"
}

/**
 * Trait Dentry describes a delegation table entry. `prefix` describes
 * the paths that the entry applies to. `dst` describes the resulting
 * tree for this prefix on lookup.
 */
case class Dentry(prefix: Dentry.Prefix, dst: NameTree[Path]) {
  def show: String = "%s=>%s".format(prefix.show, dst.show)
  override def toString: String = "Dentry(" + show + ")"
}

object Dentry {

  /** Build a [[Dentry]] with a [[Path]] prefix. */
  def apply(path: Path, dst: NameTree[Path]): Dentry =
    Dentry(Prefix(path.elems.map(Prefix.Label(_)): _*), dst)

  /**
   * Parse a Dentry from the string `s` with concrete syntax:
   * {{{
   * dentry     ::= prefix '=>' tree
   * }}}
   *
   * where the production `prefix` is from the grammar documented in
   * [[Prefix.read]] and the production `tree` is from the grammar
   * documented in [[com.twitter.finagle.NameTree.read
   * NameTree.read]].
   */
  def read(s: String): Dentry = NameTreeParsers.parseDentry(s)

  // The prefix to this is an illegal path in the sense that the
  // concrete syntax will not admit it. It will do for a no-op.
  val nop: Dentry = Dentry(Prefix(Prefix.Label("/")), NameTree.Neg)

  implicit val equiv: Equiv[Dentry] = new Equiv[Dentry] {
    def equiv(d1: Dentry, d2: Dentry): Boolean = (
      d1.prefix == d2.prefix &&
        d1.dst.simplified == d2.dst.simplified
    )
  }

  /**
   * A Prefix comprises a [[Path]]-matching expression.
   *
   * Each element in a prefix may be either a [[Dentry.Prefix.Label]]
   * or [[Dentry.Prefix.AnyElem]].
   * When matching a [[Path]], Label-elements must match exactly,
   * while Any-elements are ignored.
   */
  case class Prefix(elems: Prefix.Elem*) {

    def matches(path: Path): Boolean = {
      if (this.size > path.size)
        return false
      var i = 0
      while (i != this.size) elems(i) match {
        case Prefix.Label(buf) if buf != path.elems(i) =>
          return false
        case _ => // matches
          i += 1
      }
      true
    }

    // A prefix acts somewhat like a Seq[Elem]
    def take(n: Int): Prefix = Prefix(elems.take(n): _*)
    def drop(n: Int): Prefix = Prefix(elems.drop(n): _*)
    def ++(that: Prefix): Prefix =
      if (that.isEmpty) this
      else Prefix((elems ++ that.elems): _*)
    def size: Int = elems.size
    def isEmpty: Boolean = elems.isEmpty

    def ++(path: Path): Prefix =
      if (path.isEmpty) this
      else this ++ Prefix(path)

    lazy val showElems: Seq[String] = elems.map(_.show)
    lazy val show: String = showElems.mkString("/", "/", "")
    override def toString: String = s"""Prefix(${showElems.mkString(",")})"""
  }

  object Prefix {

    def apply(path: Path): Prefix =
      Prefix(path.elems.map(Label(_)): _*)

    /**
     * Parse `s` as a prefix matching expression with concrete syntax
     *
     * {{{
     * path       ::= '/' elems | '/'
     *
     * elems      ::= elem '/' elem | elem
     *
     * elem       ::= '*' | label
     *
     * label      ::= (\\x[a-f0-9][a-f0-9]|[0-9A-Za-z:.#$%-_])+
     *
     * }}}
     *
     * for example
     *
     * {{{
     * /foo/bar/baz
     * /foo&#47;*&#47;bar/baz
     * /
     * }}}
     *
     * parses into the path
     *
     * {{{
     * Prefix(Label(foo),Label(bar),Label(baz))
     * Prefix(Label(foo),AnyElem,Label(bar),Label(baz))
     * Prefix()
     * }}}
     *
     * @throws IllegalArgumentException when `s` is not a syntactically valid path.
     *
     * Note: There is a Java-friendly API for this method: [[Dentry.readPrefix]].
     */
    def read(s: String): Prefix = NameTreeParsers.parseDentryPrefix(s)

    val empty: Prefix = new Prefix()

    sealed trait Elem {
      def show: String
    }

    object AnyElem extends Elem {
      val show = "*"
    }

    case class Label(buf: Buf) extends Elem {
      require(!buf.isEmpty)
      lazy val show: String = Path.showElem(buf)
    }

    object Label {
      def apply(s: String): Label = Label(Buf.Utf8(s))
    }
  }

  /**
   * Java compatibility method for `Dentry.Prefix.read`
   */
  def readPrefix(s: String): Prefix = Prefix.read(s)
}

/**
 * Object Dtab manages 'base' and 'local' Dtabs.
 */
private[finagle] abstract class DtabCompanionBase {
  implicit val equiv: Equiv[Dtab] = new Equiv[Dtab] {
    def equiv(d1: Dtab, d2: Dtab): Boolean = (
      d1.size == d2.size &&
        d1.zip(d2).forall { case (de1, de2) => Equiv[Dentry].equiv(de1, de2) }
    )
  }

  /**
   * A failing delegation table.
   */
  // this needs to be lazy since DtabCompanionBase gets initialized before Dtab
  // does. once we reunify Dtab and DtabCompanionBase, we can change it back to
  // a regular val.
  lazy val fail: Dtab = Dtab.read("/=>!")

  /**
   * An empty delegation table.
   */
  val empty: Dtab = Dtab(Vector.empty)

  /**
   * An empty delegation table.
   *
   * Use this one if you're coming from Java.
   */
  // 2.13 introduces a new method `empty` on IndexedSeq which collides with the
  // static forwarder that we were previously generating in Dtab.  To avoid that
  // collision, we provide an identical field with a different identifier so the
  // static forwarder gets generated.
  val emptyDtab: Dtab = empty

  /**
   * The base, or "system", or "global", delegation table applies to
   * every request in this process. It is generally set at process
   * startup, and not changed thereafter.
   */
  @volatile var base: Dtab = empty

  /**
   * Java API for `base_=`
   */
  def setBase(dtab: Dtab): Unit =
    base = dtab

  private[this] val _local = new Local[Dtab]
  private[this] val _limited = new Local[Dtab]

  /**
   * The limited, or "non-propagated, per-request", delegation table applies to the
   * current [[com.twitter.util.Local Local]] scope which is usually
   * propagated from the upstream. Finagle uses the Dtab
   * `Dtab.base ++ Dtab.limited ++ Dtab.local` to bind [[com.twitter.finagle.Name.Path
   * Paths]] via a [[com.twitter.finagle.naming.NameInterpreter]].
   *
   * Limited's scope is dictated by [[com.twitter.util.Local Local]].
   *
   * Unlike `local`, `limited` is not propagated to the entire request graph.
   *
   */
  def limited: Dtab = _limited() match {
    case Some(dtab) => dtab
    case None => Dtab.empty
  }

  def limited_=(dtab: Dtab): Unit =
    _limited() = dtab

  /**
   * Java API for `limited_=`
   */
  def setLimited(dtab: Dtab): Unit =
    limited = dtab

  /**
   * The local, or "per-request", delegation table applies to the
   * current [[com.twitter.util.Local Local]] scope which is usually
   * defined on a per-request basis. Finagle uses the Dtab
   * `Dtab.base ++ Dtab.limited ++ Dtab.local` to bind [[com.twitter.finagle.Name.Path
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
  def local: Dtab = _local() match {
    case Some(dtab) => dtab
    case None => Dtab.empty
  }

  def local_=(dtab: Dtab): Unit =
    _local() = dtab

  /**
   * Java API for `local_=`
   */
  def setLocal(dtab: Dtab): Unit =
    local = dtab

  /**
   * `Unwind` provides an api to create scoped, local and limited Dtabs
   * and restore the original state upon exit.
   * @param f a function to be executed with the scoped Dtabs
   */
  def unwind[T](f: => T): T = {
    val saveLocal = _local()
    val saveLimited = _limited()
    try f
    finally {
      _limited.set(saveLimited)
      _local.set(saveLocal)
    }
  }

  /**
   * Parse a Dtab from string `s` with concrete syntax
   *
   * {{{
   * dtab       ::= dentry ';' dtab | dentry
   * }}}
   *
   * where the production `dentry` is from the grammar documented in
   * [[com.twitter.finagle.Dentry.read Dentry.read]]
   *
   */
  def read(s: String): Dtab = NameTreeParsers.parseDtab(s)

  /** Scala collection plumbing required to build new dtabs */
  def newBuilder: DtabBuilder = new DtabBuilder

  /**
   * implicit conversion from [[com.twitter.finagle.Dtab]] to
   * [[com.twitter.app.Flaggable]], allowing Dtabs to be easily used as
   * [[com.twitter.app.Flag]]s
   */
  implicit val flaggable: Flaggable[Dtab] = new Flaggable[Dtab] {
    override def default: Option[Dtab] = None
    def parse(s: String): Dtab = Dtab.read(s)
    override def show(dtab: Dtab): String = dtab.show
  }
}
