package com.twitter.finagle.http

import scala.collection.IterableOnce
import scala.collection.mutable

protected abstract class CookieMapVersionSpecific(message: Message, cookieCodec: CookieCodec)
    extends mutable.Map[String, Cookie] {
  def this(message: Message) = this(message, CookieMap.cookieCodec)

  protected def addCookie(cookie: (String, Cookie)): this.type
  protected def addCookies(cookies: IterableOnce[(String, Cookie)]): this.type
  protected def removeCookie(name: String): this.type
  protected def removeCookies(names: IterableOnce[String]): this.type

  override def addOne(cookie: (String, Cookie)): this.type = addCookie(cookie)
  override def addAll(cookies: IterableOnce[(String, Cookie)]): this.type = addCookies(cookies)
  override def subtractOne(name: String): this.type = removeCookie(name)
  override def subtractAll(names: IterableOnce[String]): this.type = removeCookies(names)
}
