package com.twitter.finagle.http

import org.jboss.netty.handler.codec.http.HttpHeaders
import scala.collection.JavaConverters._

/**
 * Mutable HttpMessage-backed [[HeaderMap]].
 */
private final class Netty3HeaderMap(headers: HttpHeaders) extends HeaderMap {

  def get(key: String): Option[String] =
    Option(headers.get(key))

  override def getOrNull(key: String): String =
    headers.get(key)

  def iterator: Iterator[(String, String)] =
    headers.iterator.asScala.map { entry =>
      (entry.getKey, entry.getValue)
    }

  override def keys: Iterable[String] =
    headers.names.asScala

  override def keySet: Set[String] =
    keys.toSet

  override def keysIterator: Iterator[String] =
    keys.iterator

  override def contains(key: String): Boolean =
    headers.contains(key)

  def += (kv: (String, String)): this.type = {
    set(kv._1, kv._2)
    this
  }

  def -= (key: String): this.type = {
    headers.remove(key)
    this
  }

  def getAll(key: String): Seq[String] =
    headers.getAll(key).asScala

  def set(k: String, v: String): HeaderMap = {
    headers.set(k,v)
    this
  }

  def add(k: String, v: String): HeaderMap = {
    headers.add(k, v)
    this
  }
}