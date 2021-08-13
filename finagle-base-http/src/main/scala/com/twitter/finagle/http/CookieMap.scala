package com.twitter.finagle.http

import com.twitter.finagle.http.cookie.supportSameSiteCodec
import com.twitter.finagle.http.netty4.Netty4CookieCodec
import scala.collection.mutable.ListBuffer
import scala.collection.mutable

private[finagle] object CookieMap {

  def cookieCodec = Netty4CookieCodec

  // Note that this is a def to allow it to be toggled for unit tests.
  private[finagle] def includeSameSite: Boolean = supportSameSiteCodec()
}

/**
 * Adapt cookies of a Message to a mutable Map where cookies are indexed by
 * their name. Requests use the Cookie header and Responses use the Set-Cookie
 * header. If a cookie is added to the CookieMap, a header is automatically
 * added to the Message. You can add the same cookie more than once. Use getAll
 * to retrieve all of them, otherwise only the first one is returned. If a
 * cookie is removed from the CookieMap, a header is automatically removed from
 * the ''message''
 */
class CookieMap private[finagle] (message: Message, cookieCodec: CookieCodec)
    extends CookieMapVersionSpecific(message, cookieCodec) {

  def this(message: Message) =
    this(message, CookieMap.cookieCodec)

  override def empty: CookieMap = new CookieMap(Request())

  private[this] val underlying =
    mutable.Map[String, List[Cookie]]().withDefaultValue(Nil)

  /**
   * Checks if there was a parse error. Invalid cookies are ignored.
   */
  def isValid: Boolean = _isValid
  private[this] var _isValid = true

  private[this] final def cookieHeaderName: String =
    if (message.isRequest) Fields.Cookie
    else Fields.SetCookie

  private[this] def decodeCookies(header: String): Iterable[Cookie] = {
    val decoding =
      if (message.isRequest) cookieCodec.decodeServer(header)
      else cookieCodec.decodeClient(header)
    decoding match {
      case Some(decoding) =>
        decoding
      case None =>
        _isValid = false
        Nil
    }
  }

  protected def rewriteCookieHeaders(): Unit = {
    // Clear all cookies - there may be more than one with this name.
    message.headerMap.remove(cookieHeaderName)

    // Add cookies back again if there are any
    if (nonEmpty) {
      if (message.isRequest) {
        message.headerMap.set(cookieHeaderName, cookieCodec.encodeClient(values))
      } else {
        foreach {
          case (_, cookie) =>
            message.headerMap.add(cookieHeaderName, cookieCodec.encodeServer(cookie))
        }
      }
    }
  }

  /**
   * Returns an iterator that iterates over all cookies in this map.
   */
  def iterator: Iterator[(String, Cookie)] = new Iterator[(String, Cookie)] {
    private[this] val outer: Iterator[(String, List[Cookie])] = underlying.iterator
    private[this] var inner: List[Cookie] = Nil

    def hasNext: Boolean = outer.hasNext || !inner.isEmpty
    def next(): (String, Cookie) = {
      if (inner.isEmpty) {
        inner = outer.next()._2.reverse
      }

      val result = inner.head
      inner = inner.tail
      result.name -> result
    }
  }

  /**
   * Fetches the first cookie with the given `name` from this map.
   **/
  def get(name: String): Option[Cookie] = getAll(name).headOption

  /**
   * Fetches the value of the first cookie with the given `name` from this map.
   */
  def getValue(name: String): Option[String] = get(name).map(_.value)

  /**
   * Fetches all cookies with the given `name` from this map, in order of insertion.
   */
  def getAll(name: String): Seq[Cookie] = underlying(name).reverse

  /**
   * Adds the given `cookie` (which is a tuple of cookie `name`
   * and `Cookie` itself) into this map. If there are already cookies
   * with the given `name` in the map, they will be removed.
   */
  protected def addCookie(cookie: (String, Cookie)): this.type = {
    val (n, c) = cookie
    setNoRewrite(n, c)
    rewriteCookieHeaders()
    this
  }

  /**
   * Adds the given `cookie` into this map. If there are already cookies
   * with the given `name` in the map, they will be removed.
   */
  def +=(cookie: Cookie): CookieMap = {
    this += ((cookie.name, cookie))
  }

  /**
   * Adds the given `cookies` (which are tuples of cookie `name`
   * and `Cookie` itself) into this map. If there are already cookies
   * with the given `name` in the map, they will be removed.
   */
  protected def addCookies(
    cookies: scala.collection.TraversableOnce[(String, Cookie)]
  ): this.type = {
    cookies.foreach { case (n, c) => setNoRewrite(n, c) }
    rewriteCookieHeaders()
    this
  }

  protected def removeCookies(xs: TraversableOnce[String]): this.type = {
    xs.foreach { n => underlying -= n }
    rewriteCookieHeaders()
    this
  }

  /**
   * Deletes all cookies with the given `name` from this map.
   */
  protected def removeCookie(name: String): this.type = {
    underlying -= name
    rewriteCookieHeaders()
    this
  }

  private[this] def setNoRewrite(name: String, cookie: Cookie): Unit = {
    underlying(name) = cookie :: Nil
  }

  private[this] def addNoRewrite(name: String, cookie: Cookie): Unit = {
    val prev = underlying(name)
    val next =
      if (message.isResponse && prev.contains(cookie)) cookie :: prev.filter(c => c != cookie)
      else cookie :: prev

    underlying(name) = next
  }

  /**
   * Adds the given `cookie` with `name` into this map.
   *
   * On a Response: existing cookies with this name but different domain/path
   * will be kept. If there is already an identical cookie (different value but
   * name/path/domain is the same) in the map, it will be replaced within a new
   * version.
   *
   * On a Request: existing cookies with this name will be kept.
   *
   * @see [[addAll]] for adding cookies in bulk
   */
  def add(name: String, cookie: Cookie): Unit = {
    addNoRewrite(name, cookie)
    rewriteCookieHeaders()
  }

  /**
   * Adds the given `cookie` into this map.
   *
   * On a Response: existing cookies with this name but different domain/path
   * will be kept. If there is already an identical cookie (different value but
   * name/path/domain is the same) in the map, it will be replaced within a new
   * version.
   *
   * On a Request: existing cookies with this name will be kept.
   *
   * @see [[addAll]] for adding cookies in bulk
   */
  def add(cookie: Cookie): Unit = {
    add(cookie.name, cookie)
  }

  /**
   * Adds multiple `cookies` into this map.
   *
   * On a Response: existing cookies with this name but different domain/path
   * will be kept. If there is already an identical cookie (different value but
   * name/path/domain is the same) in the map, it will be replaced within a new
   * version.
   *
   * On a Request: existing cookies with this name will be kept.
   */
  def addAll(cookies: TraversableOnce[Cookie]): Unit = {
    cookies.foreach(c => addNoRewrite(c.name, c))
    rewriteCookieHeaders()
  }

  /**
   * Removes multiple `cookies` from this map.
   */
  def removeAll(cookies: TraversableOnce[String]): Unit = {
    this --= cookies
  }

  private[this] def rewriteFirstTime(): Unit = {
    val cookies = new ListBuffer[Cookie]
    message.headerMap.getAll(cookieHeaderName).foreach { header =>
      cookies ++= decodeCookies(header)
    }
    addAll(cookies.toList)
  }

  rewriteFirstTime()
}
