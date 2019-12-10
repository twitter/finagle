package com.twitter.finagle.http

import scala.collection.mutable

protected abstract class CookieMapVersionSpecific(message: Message, cookieCodec: CookieCodec)
    extends mutable.Map[String, Cookie]
    with mutable.MapLike[String, Cookie, CookieMap] {
  def this(message: Message) = this(message, CookieMap.cookieCodec)

  protected def addCookie(cookie: (String, Cookie)): this.type
  protected def addCookies(cookies: TraversableOnce[(String, Cookie)]): this.type
  protected def removeCookie(name: String): this.type
  protected def removeCookies(names: TraversableOnce[String]): this.type

  override def +=(cookie: (String, Cookie)): this.type = addCookie(cookie)
  override def ++=(cookies: TraversableOnce[(String, Cookie)]): this.type = addCookies(cookies)
  override def -=(name: String): this.type = removeCookie(name)
  override def --=(names: TraversableOnce[String]): this.type = removeCookies(names)

  override def empty: CookieMap = new CookieMap(Request())
}
