package com.twitter.finagle.http

import com.twitter.finagle.http.cookie.SameSite
import com.twitter.finagle.http.cookie.exp.supportSameSiteCodec
import com.twitter.finagle.http.netty3.Netty3CookieCodec
import com.twitter.finagle.http.netty4.Netty4CookieCodec
import com.twitter.finagle.server.ServerInfo
import com.twitter.finagle.stats.{LoadedStatsReceiver, Verbosity}
import org.jboss.netty.handler.codec.http.HttpHeaders
import scala.collection.mutable

private[finagle] object CookieMap {

  val UseNetty4CookieCodec =
    Toggles("com.twitter.finagle.http.UseNetty4CookieCodec")

  private val cookieCodec =
    if (UseNetty4CookieCodec(ServerInfo().id.hashCode())) Netty4CookieCodec
    else Netty3CookieCodec

  // Note that this is a def to allow it to be toggled for unit tests.
  private[finagle] def includeSameSite: Boolean = supportSameSiteCodec()

  private[finagle] val flaglessSameSitesCounter =
    LoadedStatsReceiver.scope("http").scope("cookie").counter(Verbosity.Debug, "flagless_samesites")
  private[finagle] val silentlyDroppedSameSitesCounter =
    LoadedStatsReceiver.scope("http").scope("cookie").counter(Verbosity.Debug, "dropped_samesites")
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
class CookieMap private[twitter](message: Message, cookieCodec: CookieCodec)
    extends mutable.Map[String, Cookie]
    with mutable.MapLike[String, Cookie, CookieMap] {

  def this(message: Message) =
    this(message, CookieMap.cookieCodec)

  override def empty: CookieMap = new CookieMap(Request())

  private[this] val underlying =
    mutable.Map[String, Set[Cookie]]().withDefaultValue(Set.empty)

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

  protected def rewriteCookieHeaders() {
    // Clear all cookies - there may be more than one with this name.
    message.headerMap.remove(cookieHeaderName)

    // Add cookies back again
    if (message.isRequest) {
      message.headerMap.set(cookieHeaderName, cookieCodec.encodeClient(values))
    } else {
      foreach {
        case (_, cookie) => {
          message.headerMap.add(cookieHeaderName,
            cookieCodec.encodeServer(cookie))
          if (!message.headerMap.toString.contains("SameSite")
              && cookie.sameSite != SameSite.Unset) {
            CookieMap.silentlyDroppedSameSitesCounter.incr()
          }
        }
      }
    }
  }

  /**
   * Returns an iterator that iterates over all cookies in this map.
   */
  def iterator: Iterator[(String, Cookie)] =
    for {
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
    cookieHeader <- message.headerMap.getAll(cookieHeaderName)
    cookie <- decodeCookies(cookieHeader)
  } {
    // Checks whether the SameSite attribute is set in the response but the
    // codec is disabled. This is undesirable behavior so we wish to report it.
    if (!CookieMap.includeSameSite
        && message.isResponse
        && cookieHeader.contains("SameSite")) {
      CookieMap.flaglessSameSitesCounter.incr()
    }
    add(cookie)
  }
}
