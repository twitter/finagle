package com.twitter.finagle.http

import org.jboss.netty.handler.codec.http.{
  HttpHeaders,
  CookieDecoder => NettyCookieDecoder,
  CookieEncoder => NettyCookieEncoder
}
import scala.collection.mutable
import scala.collection.JavaConverters._

/**
 * Adapt cookies of a Message to a mutable Map where cookies are indexed by
 * their name. Requests use the Cookie header and Responses use the Set-Cookie
 * header. If a cookie is added to the CookieMap, a header is automatically
 * added to the Message. You can add the same cookie more than once. Use getAll
 * to retrieve all of them, otherwise only the first one is returned. If a
 * cookie is removed from the CookieMap, a header is automatically removed from
 * the ''message''
 */
class CookieMap(message: Message)
  extends mutable.Map[String, Cookie]
  with mutable.MapLike[String, Cookie, CookieMap] {
  override def empty: CookieMap = new CookieMap(Request())

  private[this] val underlying = mutable.Map[String, Set[Cookie]]().withDefaultValue(Set.empty)

  /**
   * Checks if there was a parse error. Invalid cookies are ignored.
   */
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
    message.headers.remove(cookieHeaderName)

    // Add cookies back again
    if (message.isRequest) {
      val encoder = new NettyCookieEncoder(false)
      foreach { case (_, cookie) =>
        encoder.addCookie(cookie.underlying)
      }
      message.headers.set(cookieHeaderName, encoder.encode())
    } else {
      val encoder = new NettyCookieEncoder(true)
      foreach { case (_, cookie) =>
        encoder.addCookie(cookie.underlying)
        message.headers.add(cookieHeaderName, encoder.encode())
      }
    }
  }

  /**
   * Returns an iterator that iterates over all cookies in this map.
   */
  def iterator: Iterator[(String, Cookie)] = for {
    (name, cookies) <- underlying.iterator
    cookie <- cookies
  } yield (name, cookie)

  /**
   * Applies the given function ''f'' to each cookie in this map.
   *
   * @param f a function that takes cookie ''name'' and ''Cookie'' itself
   */
  override def foreach[U](f: ((String, Cookie)) => U): Unit = iterator.foreach(f)

  /**
   * Fetches the first cookie with the given ''name'' from this map.
   *
   * @param name the cookie name
   * @return a first ''Cookie'' with the given ''name''
   **/
  def get(name: String): Option[Cookie] = getAll(name).headOption

  /**
   * Fetches the value of the first cookie with the given ''name'' from this map.
   *
   * @param name the cookie name
   * @return a value of the first cookie of the given ''name''
   */
  def getValue(name: String): Option[String] = get(name) map { _.value }

  /**
   * Fetches all cookies with the given ''name'' from this map.
   *
   * @param name the cookie name
   * @return a sequence of cookies with the same ''name''
   */
  def getAll(name: String): Seq[Cookie] = underlying(name).toSeq

  /**
   * Adds the given ''cookie'' (which is a tuple of cookie ''name''
   * and ''Cookie'' itself) into this map. If there are already cookies
   * with the given ''name'' in the map, they will be removed.
   *
   * @param cookie the tuple representing ''name'' and ''Cookie''
   */
  def +=(cookie: (String, Cookie)) = {
    val (n, c) = cookie
    underlying(n) = Set(c)
    rewriteCookieHeaders()
    this
  }

  /**
   * Adds the given  ''cookie'' into this map. If there are already cookies
   * with the given ''name'' in the map, they will be removed.
   *
   * @param cookie the ''Cookie'' to add
   */
  def +=(cookie: Cookie): CookieMap = {
    this += ((cookie.name, cookie))
  }

  /**
   * Deletes all cookies with the given ''name'' from this map.
   *
   * @param name the name of the cookies to delete
   */
  def -=(name: String) = {
    underlying -= name
    rewriteCookieHeaders()
    this
  }

  /**
   * Adds the given ''cookie'' with ''name'' into this map. Existing cookies
   * with this name but different domain/path will be kept. If there is already
   * an identical cookie (different value but name/path/domain is the same) in the
   * map, it will be replaced within a new version.
   *
   * @param name the cookie name to add
   * @param cookie the ''Cookie'' to add
   */
  def add(name: String, cookie: Cookie) {
    underlying(name) = (underlying(name) - cookie) + cookie
    rewriteCookieHeaders()
  }

  /**
   * Adds the given ''cookie'' into this map. Existing cookies with this name
   * but different domain/path will be kept. If there is already an identical
   * cookie (different value but name/path/domain is the same) in the map,
   * it will be replaced within a new version.
   *
   * @param cookie the ''Cookie'' to add
   */
  def add(cookie: Cookie) {
    add(cookie.name, cookie)
  }

  for {
    cookieHeader <- message.headers.getAll(cookieHeaderName).asScala
    cookie <- decodeCookies(cookieHeader)
  } {
    add(cookie)
  }
}
