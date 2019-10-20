package com.twitter.finagle.http

import scala.collection.IterableOnce

protected abstract class CookieMapVersionSpecific(message: Message, cookieCodec: CookieCodec) {
  def this(message: Message) = this(message, CookieMap.cookieCodec)

  def addCookie(cookie: (String, Cookie)): this.type
  def addCookies(cookies: IterableOnce[(String, Cookie)]): this.type
  def removeCookie(name: String): this.type
  def removeCookies(names: IterableOnce[String]): this.type

  def addOne(cookie: (String, Cookie)): this.type = addCookie(cookie)
  def subtractOne(name: String): this.type = removeCookie(name)

}