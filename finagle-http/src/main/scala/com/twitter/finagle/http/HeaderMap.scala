package com.twitter.finagle.http

import org.jboss.netty.handler.codec.http.HttpMessage
import scala.collection.mutable
import scala.collection.JavaConversions._


/**
 * Adapt headers of an HttpMessage to a mutable Map.  Header names
 * are case-insensitive.  For example, get("accept") is the same as
 * get("Accept").
 */
class HeaderMap(httpMessage: HttpMessage)
  extends mutable.MapLike[String, String, mutable.Map[String, String]] {
  
  def seq = Map.empty ++ iterator

  def get(key: String): Option[String] =
    Option(httpMessage.getHeader(key))

  def getAll(key: String): Iterable[String] =
    httpMessage.getHeaders(key)

  def iterator: Iterator[(String, String)] =
    httpMessage.getHeaders.toIterator map { entry =>
      (entry.getKey, entry.getValue)
    }

  override def keys: Iterable[String] =
    httpMessage.getHeaderNames

  override def contains(key: String): Boolean =
    httpMessage.containsHeader(key)

  /** Add a header but don't replace existing header(s). */
  def add(k: String, v: String) = {
    httpMessage.addHeader(k, v)
    this
  }

  /** Add a header and do replace existing header(s). */
  def += (kv: (String, String)) = {
    httpMessage.setHeader(kv._1, kv._2)
    this
  }

  /** Remove header(s). */
  def -= (key: String) = {
    httpMessage.removeHeader(key)
    this
  }

  def empty = mutable.Map[String, String]()
}
