package com.twitter.finagle

import com.twitter.io.Buf
import com.twitter.finagle.util.Showable
import java.nio.charset.Charset
import java.util.BitSet

/**
 * A Path comprises a sequence of byte buffers naming a
 * hierarchically-addressed object.
 */
case class Path(elems: Buf*) {
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
    val bytes = buf match {
      case Buf.ByteArray(bytes, 0, `nbuf`) => bytes
      case buf =>
        val bytes = new Array[Byte](nbuf)
        buf.write(bytes, 0)
        bytes
    }

    if (Path.showableAsString(bytes))
      new String(bytes, Path.Utf8Charset)
    else {
      val str = new StringBuilder(bytes.length * 4)
      var i = 0
      while (i < bytes.length) {
        str.append("\\x")
        str.append(Integer.toString((bytes(i) >> 4) & 0xf, 16))
        str.append(Integer.toString(bytes(i) & 0xf, 16))
        i += 1
      }
      str.toString
    }
  }

  lazy val show = showElems map ("/"+_) mkString ""

  override def toString = "Path("+(showElems mkString ",")+")"
}

object Path {

  implicit val showable: Showable[Path] = new Showable[Path] {
    def show(path: Path) = path.show
  }

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

  private def showableAsString(bytes: Array[Byte]): Boolean = {
    var i = 0
    while (i < bytes.size) {
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
   * path       ::= '/' labels
   * 
   * labels     ::= label '/' label
   *                label
   *
   * label      ::= (\\x[a-f0-9][a-f0-9]|[0-9A-Za-z:.#$%-_])+
   *
   * }}}
   *
   * for example
   *
   * {{{
   * /foo/bar/baz
   * }}}
   *
   * parses into the path 
   *
   * {{{
   * Path(foo,bar,baz)
   * }}}
   *
   * @throws IllegalArgumentException when `s` is not a syntactically valid path.
   */
  def read(s: String): Path = NameTreeParser(s) match {
    case NameTree.Leaf(p) => p
    case _ => throw new IllegalArgumentException("Invalid path "+s)
  }
 
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
