package com.twitter.finagle.httpx.codec

import com.google.common.io.BaseEncoding
import com.twitter.finagle._
import com.twitter.finagle.httpx.Message
import com.twitter.util.{Throw, Try, Return}
import java.nio.charset.Charset
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

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
  private val Header = "dtab-local"
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

  private val unmatchedFailure =
    Failure("Unmatched X-Dtab headers")

  private def decodingFailure(value: String) =
    Failure("Value not b64-encoded: "+value)

  private def pathFailure(path: String, cause: IllegalArgumentException) =
    Failure("Invalid path: "+path, cause)

  private def nameFailure(name: String, cause: IllegalArgumentException) =
    Failure("Invalid name: "+name, cause)

  private def decodePath(b64path: String): Try[Path] =
    b64Decode(b64path) match {
      case Throw(e: IllegalArgumentException) => Throw(decodingFailure(b64path))
      case Throw(e) => Throw(e)
      case Return(pathStr) =>
        Try(Path.read(pathStr)) rescue {
          case iae: IllegalArgumentException => Throw(pathFailure(pathStr, iae))
        }
    }

  private def decodeName(b64name: String): Try[NameTree[Path]] =
    b64Decode(b64name) match {
      case Throw(e: IllegalArgumentException) => Throw(decodingFailure(b64name))
      case Throw(e) => Throw(e)
      case Return(nameStr) =>
        Try(NameTree.read(nameStr)) rescue {
          case iae: IllegalArgumentException => Throw(nameFailure(nameStr, iae))
        }
    }

  private def validHeaderPair(aKey: String, bKey: String): Boolean =
    aKey.length == bKey.length &&
    aKey(aKey.length - 1) == 'a' &&
    bKey(bKey.length - 1) == 'b' &&
    aKey.substring(0, aKey.length - 1) == bKey.substring(0, bKey.length - 1)

  private val EmptyReturn = Return(Dtab.empty)

  def clear(msg: Message) {
    val names = msg.headers.names.iterator()
    msg.headers.remove(Header)
    while (names.hasNext) {
      val n = names.next()
      if (n.toLowerCase startsWith Prefix)
        msg.headers.remove(n)
    }
  }

  def write(dtab: Dtab, msg: Message) {
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
      msg.headers.set(Prefix+indexstr(i)+"-B", b64Encode(dst.show))
    }
  }

  /**
   * Parse old-style X-Dtab pairs and then new-style Dtab-Local headers,
   * Dtab-Local taking precedence.
   */
  def read(msg: Message): Try[Dtab] =
    for {
      dtab0 <- readXDtabPairs(msg)
      dtab1 <- readDtabLocal(msg)
    } yield dtab0 ++ dtab1

  /**
   * Parse Dtab-Local headers into a Dtab.
   *
   * If multiple Dtab-Local headers are present, they are concatenated.
   * A Dtab-Local header may contain a comma-separated list of Dtabs.
   *
   * N.B. Comma is not a showable character in Paths nor is it meaningful in Dtabs.
   */
  private def readDtabLocal(msg: Message): Try[Dtab] =
    if (!msg.headers.contains(Header)) EmptyReturn else Try {
      val headers = msg.headers().getAll(Header).asScala
      val dentries = headers.view flatMap(_ split ',') flatMap(Dtab.read(_))
      Dtab(dentries.toIndexedSeq)
    }

  /**
   * Parse header pairs into a Dtab:
   *   X-Dtab-00-A: base64(/prefix)
   *   X-Dtab-00-B: base64(/dstA & /dstB)
   */
  private def readXDtabPairs(msg: Message): Try[Dtab] = {
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

    if (keys == null)
      return EmptyReturn

    if (keys.size % 2 != 0)
      return Throw(unmatchedFailure)

    keys = keys.sorted
    val n = keys.size/2

    val dentries = new Array[Dentry](n)
    var i = 0
    while (i < n) {
      val j = i*2
      val prefix = keys(j)
      val dest = keys(j+1)

      if (!validHeaderPair(prefix, dest))
        return Throw(unmatchedFailure)

      val tryDentry =
        for {
          path <- decodePath(msg.headers.get(prefix))
          name <- decodeName(msg.headers.get(dest))
        } yield Dentry(path,  name)

      dentries(i) =
        tryDentry match {
          case Return(dentry) => dentry
          case Throw(e) =>
            return Throw(e)
        }

      i += 1
    }

    Return(Dtab(dentries))
  }
}
