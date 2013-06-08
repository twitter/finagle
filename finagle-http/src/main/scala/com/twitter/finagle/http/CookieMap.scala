package com.twitter.finagle.http

import org.jboss.netty.handler.codec.http.{HttpHeaders,
  CookieDecoder => NettyCookieDecoder, CookieEncoder => NettyCookieEncoder}
import scala.collection.mutable
import scala.collection.JavaConverters._

/**
 * Adapt cookies of a Message to a mutable Map where cookies are indexed by
 * their name. Requests use the Cookie header and Responses use the Set-Cookie
 * header. If a cookie is added to the CookieMap, a header is automatically
 * added to the Message. You can add the same cookie more than once. Use getAll
 * to retrieve all of them, otherwise only the first one is returned. If a
 * cookie is removed from the CookieMap, a header is automatically removed from
 * the message.
 */
class CookieMap(message: Message)
  extends mutable.Map[String, Cookie]
  with mutable.MapLike[String, Cookie, CookieMap] {
  override def empty: CookieMap = new CookieMap(Request())

  private[this] val underlying = mutable.Map[String, Seq[Cookie]]()

  /** Check if there was a parse error. Invalid cookies are ignored. */
  def isValid = _isValid
  private[this] var _isValid = true

  private[this] val cookieHeaderName =
    if (message.isRequest)
      HttpHeaders.Names.COOKIE
    else
      HttpHeaders.Names.SET_COOKIE

  private[this] def decodeCookies(header: String): Iterable[Cookie] = {
    val decoder = new NettyCookieDecoder
    try {
      decoder.decode(header).asScala map { new Cookie(_) }
    } catch {
      case e: IllegalArgumentException =>
        _isValid = false
        Nil
    }
  }

  protected def rewriteCookieHeaders() {
    // Clear all cookies - there may be more than one with this name.
    message.removeHeader(cookieHeaderName)

    // Add cookies back again
    foreach { case (_, cookie) =>
      val encoder = new NettyCookieEncoder(message.isResponse)
      encoder.addCookie(cookie.underlying)
      message.addHeader(cookieHeaderName, encoder.encode())
    }
  }

  /** Iterate through all cookies. */
  def iterator: Iterator[(String, Cookie)] = {
    for {
      (name, cookies) <- underlying.iterator
      cookie <- cookies
    } yield (name, cookie)
  }

  /** Get first cookie with this name. */
  def get(key: String): Option[Cookie] = getAll(key).headOption
  def getValue(key: String): Option[String] = get(key) map { _.value }

  /** Get all cookies with this name. */
  def getAll(key: String): Seq[Cookie] = underlying.getOrElse(key, Nil)

  /** Add cookie. Remove existing cookies with this name. */
  def +=(kv: (String, Cookie)) = {
    underlying(kv._1) = Seq(kv._2)
    rewriteCookieHeaders()
    this
  }

  def +=(cookie: Cookie): CookieMap = {
    this += ((cookie.name, cookie))
  }

  /** Delete all cookies with this name. */
  def -=(key: String) = {
    underlying -= key
    rewriteCookieHeaders()
    this
  }

  /** Add cookie. Keep existing cookies with this name. */
  def add(k: String, v: Cookie) {
    underlying(k) = underlying.getOrElse(k, Nil) :+ v
    rewriteCookieHeaders()
  }

  def add(cookie: Cookie) {
    add(cookie.name, cookie)
  }

  for {
    cookieHeader <- message.getHeaders(cookieHeaderName).asScala
    cookie <- decodeCookies(cookieHeader)
  } {
    add(cookie)
  }
}
