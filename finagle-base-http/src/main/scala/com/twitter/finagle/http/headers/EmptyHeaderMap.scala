package com.twitter.finagle.http.headers

import com.twitter.finagle.http.HeaderMap

/**
 * An empty and read-only [[HeaderMap]]. Use [[HeaderMap.Empty]] for a singleton instance.
 */
private[http] final class EmptyHeaderMap extends HeaderMap {
  def getAll(key: String): Seq[String] = Nil
  def get(key: String): Option[String] = None
  def add(k: String, v: String): this.type = this
  def addUnsafe(k: String, v: String): this.type = this
  def set(k: String, v: String): this.type = this
  def setUnsafe(k: String, v: String): this.type = this
  def removeHeader(k: String): this.type = this
  def iterator: Iterator[(String, String)] = Iterator.empty

  override def isEmpty: Boolean = true
  override def toString: String = "EmptyHeaderMap"
}
