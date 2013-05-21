package com.twitter.finagle.http

import org.jboss.netty.handler.codec.http.{Cookie, CookieDecoder, CookieEncoder, HttpHeaders}
import scala.collection.mutable
import scala.collection.JavaConverters._


/**
 * Adapt cookies of a Message to a mutable Set.  Requests use the Cookie header and
 * Responses use the Set-Cookie header.  If a cookie is added to the CookieSet, a
 * header is automatically added to the Message.  If a cookie is removed from the
 * CookieSet, a header is automatically removed from the message.
 *
 * Note: This is a Set, not a Map, because we assume the caller should choose the
 * cookie based on name, domain, path, and possibly other attributes.
 */
class CookieSet(message: Message) extends
  mutable.SetLike[Cookie, mutable.Set[Cookie]] {

  def seq = Set.empty ++ iterator

  private[this] var _isValid = true

  private[this] val cookieHeaderName =
    if (message.isRequest)
      HttpHeaders.Names.COOKIE
    else
      HttpHeaders.Names.SET_COOKIE

  private[this] val cookies: mutable.Set[Cookie] = {
    val decoder = new CookieDecoder
    mutable.Set[Cookie]() ++
      message.getHeaders(cookieHeaderName).asScala.map { cookieHeader =>
        try {
          decoder.decode(cookieHeader).asScala.toList
        } catch {
          case e: IllegalArgumentException =>
            _isValid = false
            Nil
        }
      }.flatten
  }

  /** Check if there was a parse error.  Invalid cookies are ignored. */
  def isValid = _isValid

  def +=(cookie: Cookie) = {
    cookies += cookie
    rewriteCookieHeaders()
    this
  }

  def -=(cookie: Cookie) = {
    cookies -= cookie
    rewriteCookieHeaders()
    this
  }

  def contains(cookie: Cookie) =
    cookies.contains(cookie)

  def iterator = cookies iterator

  def empty = mutable.Set[Cookie]()

  protected def rewriteCookieHeaders() {
    // Clear all cookies - there may be more than one with this name.
    message.removeHeader(cookieHeaderName)

    // Add cookies back again
    cookies foreach { cookie =>
      val cookieEncoder = new CookieEncoder(message.isResponse)
      cookieEncoder.addCookie(cookie)
      message.addHeader(cookieHeaderName, cookieEncoder.encode())
    }
  }
}
