package com.twitter.finagle.http.codec

import com.twitter.finagle.{Dtab, Dentry, NameTree, Path}
import org.jboss.netty.handler.codec.http.HttpMessage
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.base64.Base64
import scala.collection.mutable.ArrayBuffer
import java.nio.charset.Charset

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
  private val Ascii = Charset.forName("ASCII") 

  private val indexstr: Int => String =
    ((0 until Maxsize) map(i => i -> "%02d".format(i))).toMap
  
  private def encodeValue(v: String): String = {
    val buf = ChannelBuffers.wrappedBuffer(v.getBytes(Utf8))
    val breakLines = false     // don't want newlines in http headers
    val buf64 = Base64.encode(buf, breakLines)
    buf64.toString(Ascii)
  }
  
  private def decodeValue(v: String): String = {
    val buf64 = ChannelBuffers.wrappedBuffer(v.getBytes(Ascii))
    val buf = Base64.decode(buf64)
    buf.toString(Utf8)
  }

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
      msg.headers.set(Prefix+indexstr(i)+"-A", encodeValue(prefix.show))
      msg.headers.set(Prefix+indexstr(i)+"-B".format(i), encodeValue(dst.show))
    }
  }

  def read(msg: HttpMessage): Dtab = {
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

    if (keys == null) return Dtab.empty

    keys = keys.sorted
    if (keys.size % 2 != 0)
      return Dtab.empty

    val n = keys.size/2

    val dentries = new Array[Dentry](n)
    var i = 0
    while (i < n) {
      val j = i*2
      val prefix = keys(j)
      val dest = keys(j+1)

      if (prefix.size != dest.size)
        return Dtab.empty

      if (prefix.substring(0, prefix.size-1) != dest.substring(0, dest.size-1))
        return Dtab.empty
      if (prefix(prefix.size-1) != 'a' || dest(dest.size-1) != 'b')
        return Dtab.empty

      dentries(i) =  
        try {
          val src = Path.read(decodeValue(msg.headers.get(prefix)))
          val dst = NameTree.read(decodeValue(msg.headers.get(dest)))
          Dentry(src, dst)
        } catch {
          case _: IllegalArgumentException =>
            return Dtab.empty
        }

      i += 1
    }

    Dtab(dentries)
  }
}
