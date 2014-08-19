package com.twitter.finagle.http.codec

import com.google.common.io.BaseEncoding
import com.twitter.finagle._
import com.twitter.util.{Throw, Try, Return}
import java.nio.charset.Charset
import org.jboss.netty.handler.codec.http.HttpMessage
import scala.collection.mutable.ArrayBuffer

/**
 * Dtab serialization for Http. Dtabs are encoded into Http
 * headers with keys
 *	x-dtab-$idx-(a|b)
 * where $idx is a two-digit integer. These headers are encoded in
 * pairs: 'a' and 'b' headers must exist for each index. Thus when
 * header names are lexically sorted, Dtab entries are decoded
 * pairwise. 'a' denoting prefix, 'b' destination.
 *
 * Header values are base64-encoded ("standard" alphabet)
 * Utf8 strings.
 */
object HttpDtab {
  private val Prefix = "x-dtab-"
  private val Maxsize = 100
  private val Utf8 = Charset.forName("UTF-8")
  private val Base64 = BaseEncoding.base64()

  private val indexstr: Int => String =
    ((0 until Maxsize) map (i => i -> "%02d".format(i))).toMap

  private def b64Encode(v: String): String =
    Base64.encode(v.getBytes(Utf8))

  private def b64Decode(v: String): Try[String] =
    Try { Base64.decode(v) } map(new String(_, Utf8))

  def clear(msg: HttpMessage) {
    val names = msg.headers.names.iterator()
    while (names.hasNext()) {
      val n = names.next()
      if (n.toLowerCase startsWith Prefix)
        msg.headers.remove(n)
    }
  }

  def write(dtab: Dtab, msg: HttpMessage) {
    if (dtab.isEmpty)
      return
    
    if (dtab.size >= Maxsize) {
      throw new IllegalArgumentException(
        "Dtabs with length greater than 100 are not serializable with HTTP")
    }

    for ((Dentry(prefix, dst), i) <- dtab.zipWithIndex) {
      // TODO: now that we have a proper Dtab grammar,
      // should just embed this directly instead.
      msg.headers.set(Prefix+indexstr(i)+"-A", b64Encode(prefix.show))
      msg.headers.set(Prefix+indexstr(i)+"-B".format(i), b64Encode(dst.show))
    }
  }

  def read(msg: HttpMessage): Try[Dtab] = {
    // Common case: no actual overrides.
    var keys: ArrayBuffer[String] = null
    val headers = msg.headers.iterator()
    while (headers.hasNext()) {
      val key = headers.next().getKey().toLowerCase
      if (key startsWith Prefix) {
        if (keys == null) keys = ArrayBuffer[String]()
        keys += key
      }
    }

    if (keys == null) return Return(Dtab.empty)

    keys = keys.sorted
    if (keys.size % 2 != 0)
      return Throw(new UnmatchedHeaderException)

    val n = keys.size/2

    val dentries = new Array[Dentry](n)
    var i = 0
    while (i < n) {
      val j = i*2
      val prefix = keys(j)
      val dest = keys(j+1)
      if (prefix.size != dest.size ||
          prefix.substring(0, prefix.size-1) != dest.substring(0, dest.size-1) ||
          prefix(prefix.size-1) != 'a' || dest(dest.size-1) != 'b')
        return Throw(new UnmatchedHeaderException)

      val b64src = msg.headers.get(prefix)
      val src = b64Decode(b64src) match {
        case Throw(e: IllegalArgumentException) =>
          return Throw(new HeaderDecodingException(b64src))
        case Throw(e) =>
          return Throw(e)

        case Return(pathStr) =>
          try Path.read(pathStr) catch {
            case iae: IllegalArgumentException =>
              return Throw(new InvalidPathException(pathStr, iae.getMessage))
          }
      }

      val b64dst = msg.headers.get(dest)
      val dst = b64Decode(b64dst) match {
        case Throw(e: IllegalArgumentException) =>
          return Throw(new HeaderDecodingException(b64dst))
        case Throw(e) =>
          return Throw(e)

        case Return(nameStr) =>
          try NameTree.read(nameStr) catch {
            case iae: IllegalArgumentException =>
              return Throw(new InvalidNameException(nameStr, iae.getMessage))
          }
      }

      dentries(i) = Dentry(src, dst)
      i += 1
    }

    Return(Dtab(dentries))
  }

  /*
   * X-Dtab header parsing errors
   */

  sealed class HeaderException(msg: String)
    extends Exception(msg)
    with NoStacktrace

  class UnmatchedHeaderException
    extends HeaderException("Unmatched X-Dtab headers")

  class HeaderDecodingException(value: String)
    extends HeaderException("Value not b64-encoded: "+value)

  class InvalidPathException(path: String, msg: String)
    extends HeaderException("Invalid path: "+path+": "+msg)

  class InvalidNameException(name: String, msg: String)
    extends HeaderException("Invalid name: "+name+": "+msg)
}
