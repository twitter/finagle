package com.twitter.finagle.http

protected abstract class HeaderMapVersionSpecific {
  def set(name: String, header: String): this.type
  def removeHeader(name: String): this.type

  def -=(key: String): this.type = removeHeader(key)
  def +=(kv: (String, String)): this.type = set(kv._1, kv._2)
}
