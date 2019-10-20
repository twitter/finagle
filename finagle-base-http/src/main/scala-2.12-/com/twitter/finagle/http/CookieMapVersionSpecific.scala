
package com.twitter.finagle.http

import scala.collection.mutable.Builder
import scala.collection.mutable.MapLike

protected abstract class CookieMapVersionSpecific(message: Message, cookieCodec: CookieCodec) {
  def this(message: Message) = this(message, CookieMap.cookieCodec)

  def addCookie(cookie: (String, Cookie)): this.type
  def addCookies(cookies: TraversableOnce[(String, Cookie)]): this.type
  def removeCookie(name: String): this.type
  def removeCookies(names: TraversableOnce[String]): this.type

  def +=(cookie: (String, Cookie)): this.type = addCookie(cookie)
  def -=(name: String): this.type = removeCookie(name)
}