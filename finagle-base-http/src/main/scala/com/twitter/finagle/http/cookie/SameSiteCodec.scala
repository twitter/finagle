package com.twitter.finagle.http.cookie

import com.twitter.finagle.http.Cookie
import com.twitter.finagle.stats.{LoadedStatsReceiver, Verbosity}
import com.twitter.logging.Logger
import io.netty.handler.codec.http.CookieDecoder
import java.lang.reflect.Method
import java.util.{ArrayList => AList, List => JList}

/**
 * Encodes and decodes the SameSite Set-Cookie attribute to Strings and to
 * Cookies, respectively.
 */
private[http] object SameSiteCodec {

  private val log: Logger = Logger()
  private val cookieDecoderClass = classOf[CookieDecoder]
  private val initFailureCounter =
    LoadedStatsReceiver.scope("http").scope("cookie").counter(Verbosity.Debug, "samesite_failures")

  private val _extractKeyValuePairs: Option[Method] =
    try {
      val method = cookieDecoderClass.getDeclaredMethod(
        "extractKeyValuePairs",
        classOf[String],
        classOf[JList[String]],
        classOf[JList[String]]
      )
      method.setAccessible(true)
      Some(method)
    } catch {
      case t: Throwable =>
        log.error(t, "Failed to initialize `_extractKeyValuePairs`.")
        None
    }

  /**
   * Provides access to `CookieDecoder.extractKeyValuePairs` within Netty. It is
   * a private static method which we change the visibility on in order to reuse
   * for parsing the cookie header again. Cookie header parsing is error prone and
   * not code we wish to rewrite.
   */
  private def extractKeyValuePairs(
    header: String,
    names: JList[String],
    values: JList[String]
  ): Unit = {
    _extractKeyValuePairs match {
      case Some(method) => method.invoke(null /* static method */, header, names, values)
      case None => ()
    }
  }

  /**
   * Adds the `SameSite` cookie attribute to a header value which has already been encoded
   * from an existing Finagle `Cookie`.
   */
  def encodeSameSite(cookie: Cookie, encoded: String): String = cookie.sameSite match {
    case SameSite.Lax => encoded + "; SameSite=Lax"
    case SameSite.Strict => encoded + "; SameSite=Strict"
    case SameSite.None => encoded + "; SameSite=None"
    case _ => encoded
  }

  /**
   * Decodes the `SameSite` cookie attribute and adds it to an existing Finagle
   * `Cookie` value.
   */
  def decodeSameSite(header: String, cookie: Cookie): Cookie = {
    val names: AList[String] = new AList[String]()
    val values: AList[String] = new AList[String]()

    try {
      extractKeyValuePairs(header, names, values)
    } catch {
      case t: Throwable =>
        log.warning(t, "Failed to extract attributes from header.")
        initFailureCounter.incr()
        return cookie
    }

    var index: Int = 0
    val len: Int = names.size()
    var pos: Int = -1

    while (index < len && pos == -1) {
      if (names.get(index).equalsIgnoreCase("samesite")) {
        pos = index
      }
      index += 1
    }

    if (pos <= -1) {
      cookie
    } else {
      val sameSite =
        if (values.get(pos).equalsIgnoreCase("lax")) SameSite.Lax
        else if (values.get(pos).equalsIgnoreCase("strict")) SameSite.Strict
        else if (values.get(pos).equalsIgnoreCase("none")) SameSite.None
        else SameSite.Unset

      cookie.sameSite(sameSite)
    }
  }

}
