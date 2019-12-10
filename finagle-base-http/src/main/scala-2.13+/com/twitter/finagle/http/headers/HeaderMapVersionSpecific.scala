package com.twitter.finagle.http

import com.twitter.finagle.http.headers.Header
import scala.collection.mutable

protected abstract class HeaderMapVersionSpecific {
  def set(name: String, header: String): this.type
  def removeHeader(name: String): this.type

  def subtractOne(key: String): this.type = removeHeader(key)
  def addOne(kv: (String, String)): this.type = set(kv._1, kv._2)
}
