package com.twitter.finagle.http.headers

import com.twitter.finagle.http
import com.twitter.finagle.http.HeaderMap

/**
 * Mutable, thread-safe [[HeaderMap]] implementation backed by a
 * HeaderHash.
 */
private final class HashBackedHeaderMap extends http.HeaderMap {
  import HeaderMap._

  // In general, HashSet/HashTables that are not thread safe are not
  // durable to concurrent modification and can result in infinite loops.
  // As such, we synchronize on the underlying `Headers` when performing
  // accesses to avoid this. In the common case of no concurrent access,
  // this should be cheap.
  private[this] val underlying = new HeadersHash

  // ---- HeaderMap -----

  def getAll(key: String): Seq[String] = underlying.synchronized {
    underlying.getAll(key)
  }

  // Validates key and value.
  def add(key: String, value: String): HeaderMap = {
    validateName(key)
    addUnsafe(key, foldReplacingValidateValue(key, value))
  }

  // Does not validate key and value.
  def addUnsafe(key: String, value: String): HeaderMap = underlying.synchronized {
    underlying.add(key, value)
    this
  }

  // Validates key and value.
  def set(key: String, value: String): HeaderMap = {
    validateName(key)
    setUnsafe(key, foldReplacingValidateValue(key, value))
  }

  // Does not validate key and value.
  def setUnsafe(key: String, value: String): HeaderMap = underlying.synchronized {
    underlying.set(key, value)
    this
  }

  // Overriding this for efficiency reasons.
  override def getOrNull(key: String): String = {
    underlying.getFirstOrNull(key)
  }

  // ---- Map/MapLike -----

  def get(key: String): Option[String] = underlying.synchronized {
    underlying.getFirst(key)
  }

  def iterator: Iterator[(String, String)] = underlying.synchronized {
    underlying.flattenIterator
  }

  def +=(kv: (String, String)): this.type = {
    set(kv._1, kv._2)
    this
  }

  def -=(key: String): this.type = underlying.synchronized {
    underlying.removeAll(key)
    this
  }

  override def keysIterator: Iterator[String] = underlying.synchronized {
    underlying.uniqueNamesIterator
  }

  private[finagle] override def nameValueIterator: Iterator[HeaderMap.NameValue] =
    underlying.synchronized {
      underlying.flattenedNameValueIterator
    }
}

private object HashBackedHeaderMap {

  /** Construct a new `HeaderMap` with the header list
   *
   * @note the headers are added to this `HeaderMap` via an `add` operation.
   */
  def apply(headers: (String, String)*): HeaderMap = {
    val result = new HashBackedHeaderMap
    headers.foreach(t => result.add(t._1, t._2))
    result
  }
}
