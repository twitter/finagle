package com.twitter.finagle

import com.twitter.app.Flaggable
import com.twitter.io.Buf
import com.twitter.finagle.util.Showable
import java.nio.charset.Charset
import java.util.BitSet

/**
 * A Path comprises a sequence of byte buffers naming a
 * hierarchically-addressed object.
 */
case class Path(elems: Buf*) {
  require(elems.forall(Path.nonemptyBuf))

  def startsWith(other: Path) = elems startsWith other.elems

  def take(n: Int) = Path((elems take n):_*)
  def drop(n: Int) = Path((elems drop n):_*)
  def ++(that: Path) =
    if (that.isEmpty) this
    else Path((elems ++ that.elems):_*)
  def size = elems.size
  def isEmpty = elems.isEmpty

  lazy val showElems = elems map { buf =>
    // We're extra careful with allocation here because any time
    // there are nonbase delegations, we need to serialize the paths
    // to strings
    val nbuf = buf.length
    val bytes = Buf.ByteArray.Owned.extract(buf)
    if (Path.showableAsString(bytes, nbuf))
      new String(bytes, 0, nbuf, Path.Utf8Charset)
    else {
      val str = new StringBuilder(nbuf * 4)
      var i = 0
      while (i < nbuf) {
        str.append("\\x")
        str.append(Integer.toString((bytes(i) >> 4) & 0xf, 16))
        str.append(Integer.toString(bytes(i) & 0xf, 16))
        i += 1
      }
      str.toString
    }
  }

  lazy val show = "/"+(showElems mkString "/")

  override def toString = "Path("+(showElems mkString ",")+")"
}

object Path {
  private val nonemptyBuf: Buf => Boolean = !_.isEmpty

  implicit val showable: Showable[Path] = new Showable[Path] {
    def show(path: Path) = path.show
  }

  /**
   * implicit conversion from [[com.twitter.finagle.Path]] to
   * [[com.twitter.app.Flaggable]], allowing Paths to be easily used as
   * [[com.twitter.app.Flag]]s
   */
  implicit val flaggable: Flaggable[Path] = new Flaggable[Path] {
    override def default = None
    def parse(s: String) = Path.read(s)
    override def show(path: Path) = path.show
  }

  val empty = Path()

  private val Utf8Charset = Charset.forName("UTF-8")

  val showableChars: Seq[Char] =
    ('0' to '9') ++ ('A' to 'Z') ++ ('a' to 'z') ++ "_:.#$%-".toSeq

  private val charSet = {
    val bits = new BitSet(Byte.MaxValue+1)
    for (c <- showableChars)
      bits.set(c.toInt)
    bits
  }

  /**
   * Path elements follow the lexical convention of DNS, plus a few
   * extensions: "protocols mandate that component hostname labels
   * may contain only the ASCII letters 'a' through 'z' (in a
   * case-insensitive manner), the digits '0' through '9', and the
   * hyphen ('-')."
   */
  def isShowable(ch: Char): Boolean = charSet.get(ch.toInt)

  private def showableAsString(bytes: Array[Byte], size: Int): Boolean = {
    var i = 0
    while (i < size) {
      if (!isShowable(bytes(i).toChar))
        return false
      i += 1
    }
    true
  }

  /**
   * Parse `s` as a path with concrete syntax
   *
   * {{{
   * path       ::= '/' labels | '/'
   *
   * labels     ::= label '/' labels | label
   *
   * label      ::= (\\x[a-f0-9][a-f0-9]|[0-9A-Za-z:.#$%-_])+
   *
   * }}}
   *
   * for example
   *
   * {{{
   * /foo/bar/baz
   * /
   * }}}
   *
   * parses into the path
   *
   * {{{
   * Path(foo,bar,baz)
   * Path()
   * }}}
   *
   * @throws IllegalArgumentException when `s` is not a syntactically valid path.
   */
  def read(s: String): Path = NameTreeParsers.parsePath(s)

 /**
  * Utilities for constructing and pattern matching over
  * Utf8-typed paths.
  */
  object Utf8 {
    def apply(elems: String*): Path = {
      val elems8 = elems map { el => Buf.Utf8(el) }
      Path(elems8:_*)
    }

    def unapplySeq(path: Path): Option[Seq[String]] = {
      val Path(elems@_*) = path

      val n = elems.size
      val elemss = new Array[String](n)
      var i = 0
      while (i < n) {
        elems(i) match {
          case Buf.Utf8(s) =>
            elemss(i) = s
          case _ =>
            return None
        }
        i += 1
      }

      Some(elemss)
    }
  }
}
