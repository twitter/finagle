package com.twitter.finagle.http.codec

import com.twitter.finagle._
import com.twitter.finagle.http.{HeaderMap, Message}
import com.twitter.util.{Return, Throw, Try}
import java.nio.charset.StandardCharsets.{US_ASCII, UTF_8}
import java.util.Base64
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

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
  private val Base64Encoder = Base64.getEncoder()
  private val Base64Decoder = Base64.getDecoder()

  private def b64Encode(v: String): String = {
    val encoded = Base64Encoder.encode(v.getBytes(UTF_8))
    new String(encoded, US_ASCII)
  }

  private def b64Decode(v: String): Try[String] = {
    try {
      val decoded = Base64Decoder.decode(v)
      Return(new String(decoded, UTF_8))
    } catch {
      case NonFatal(t) => Throw(t)
    }
  }

  private val unmatchedFailure =
    Failure("Unmatched X-Dtab headers")

  private def decodingFailure(value: String) =
    Failure("Value not b64-encoded: " + value)

  private def prefixFailure(prefix: String, cause: IllegalArgumentException) =
    Failure("Invalid prefix: " + prefix, cause)

  private def nameFailure(name: String, cause: IllegalArgumentException) =
    Failure("Invalid name: " + name, cause)

  private def decodePrefix(b64pfx: String): Try[Dentry.Prefix] =
    b64Decode(b64pfx) match {
      case Throw(_: IllegalArgumentException) => Throw(decodingFailure(b64pfx))
      case Throw(e) => Throw(e)
      case Return(pfxStr) =>
        Try(Dentry.Prefix.read(pfxStr)).rescue {
          case iae: IllegalArgumentException => Throw(prefixFailure(pfxStr, iae))
        }
    }

  private def decodeName(b64name: String): Try[NameTree[Path]] =
    b64Decode(b64name) match {
      case Throw(_: IllegalArgumentException) => Throw(decodingFailure(b64name))
      case Throw(e) => Throw(e)
      case Return(nameStr) =>
        Try(NameTree.read(nameStr)).rescue {
          case iae: IllegalArgumentException => Throw(nameFailure(nameStr, iae))
        }
    }

  private def validHeaderPair(aKey: String, bKey: String): Boolean =
    aKey.length == bKey.length &&
      aKey(aKey.length - 1) == 'a' &&
      bKey(bKey.length - 1) == 'b' &&
      aKey.substring(0, aKey.length - 1) == bKey.substring(0, bKey.length - 1)

  private def isDtabHeader(hdr: HeaderMap.NameValue): Boolean =
    hdr.name.equalsIgnoreCase(Header) ||
      hdr.name.regionMatches(true, 0, Prefix, 0, Prefix.length)

  private val EmptyReturn = Return(Dtab.empty)

  /**
   * Strip out and return a sequence of all Dtab-related headers in the given message.
   *
   * @return a Seq[(String, String)] containing the dtab header entries found.
   */
  private[finagle] def strip(msg: Message): Seq[(String, String)] = {
    var headerArr: ArrayBuffer[(String, String)] = null
    val nameValueIt = msg.headerMap.nameValueIterator
    while (nameValueIt.hasNext) {
      val nameValue = nameValueIt.next()
      if (isDtabHeader(nameValue)) {
        if (headerArr == null)
          headerArr = new ArrayBuffer[(String, String)]()
        headerArr += ((nameValue.name, nameValue.value))
        msg.headerMap -= nameValue.name
      }
    }
    if (headerArr == null) Nil
    else headerArr.toSeq
  }

  /**
   * Clear all Dtab-related headers from the given message.
   */
  def clear(msg: Message): Unit = {
    val headerIt = msg.headerMap.nameValueIterator
    while (headerIt.hasNext) {
      val header = headerIt.next()
      if (isDtabHeader(header))
        msg.headerMap -= header.name
    }
  }

  /**
   * Write a Dtab-local header into the given message.
   */
  def write(dtab: Dtab, msg: Message): Unit = {
    if (dtab.isEmpty)
      return

    if (dtab.size >= Maxsize) {
      throw new IllegalArgumentException(
        "Dtabs with length greater than 100 are not serializable with HTTP"
      )
    }

    msg.headerMap.set(Header, dtab.show)
  }

  /**
   * Parse old-style X-Dtab pairs and then new-style Dtab-Local headers,
   * Dtab-Local taking precedence.
   *
   * @return a Try[Dtab] which keeps the created Dtab if reading in worked.
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
    if (!msg.headerMap.contains(Header)) EmptyReturn
    else
      Try {
        val headers = msg.headerMap.getAll(Header)
        val dentries = headers.view.flatMap(_ split ',').flatMap(Dtab.read(_))
        Dtab(dentries.toIndexedSeq)
      }

  /**
   * Parse header pairs into a Dtab:
   *   X-Dtab-00-A: base64(/prefix)
   *   X-Dtab-00-B: base64(/dstA & /dstB)
   */
  private def readXDtabPairs(msg: Message): Try[Dtab] = {
    // Common case: no actual overrides.
    var builder: ArrayBuffer[String] = null
    val headers = msg.headerMap.nameValueIterator
    while (headers.hasNext) {
      val key = headers.next().name.toLowerCase
      if (key.startsWith(Prefix)) {
        if (builder == null) builder = new ArrayBuffer[String]()
        builder += key
      }
    }

    if (builder == null)
      return EmptyReturn

    val keys = builder.mapResult(as => as.sorted).result
    if (keys.size % 2 != 0)
      return Throw(unmatchedFailure)

    val n = keys.size / 2

    val dentries = new Array[Dentry](n)
    var i = 0
    while (i < n) {
      val j = i * 2
      val prefix = keys(j)
      val dest = keys(j + 1)

      if (!validHeaderPair(prefix, dest))
        return Throw(unmatchedFailure)

      val tryDentry =
        for {
          pfx <- decodePrefix(msg.headerMap(prefix))
          name <- decodeName(msg.headerMap(dest))
        } yield Dentry(pfx, name)

      dentries(i) = tryDentry match {
        case Return(dentry) => dentry
        case Throw(e) =>
          return Throw(e)
      }

      i += 1
    }

    Return(Dtab(dentries))
  }
}
