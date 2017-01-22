package com.twitter.finagle.netty4.http

import com.twitter.finagle.http.HeaderMap
import io.netty.handler.codec.http.HttpHeaders
import java.util.Map.Entry
import scala.collection.JavaConverters._

/**
 * [[HeaderMap]] implementation which proxies all calls to a
 * mutable netty `HttpHeaders` instance.
 */
private[http] class Netty4HeaderMap(private[http] val underlying: HttpHeaders) extends HeaderMap {
  import Netty4HeaderMap._

  def getAll(key: String): Iterable[String] = underlying.getAll(key).asScala

  def get(key: String): Option[String] = Option(underlying.get(key))

  override def getOrNull(key: String): String = underlying.get(key)

  def set(k: String, v: String): HeaderMap = {
    underlying.set(k, v)
    this
  }

  def add(k: String, v: String): HeaderMap = {
    underlying.add(k, v)
    this
  }

  def +=(kv: (String, String)): Netty4HeaderMap.this.type = {
    underlying.add(kv._1, kv._2)
    this
  }

  def -=(key: String): Netty4HeaderMap.this.type = {
    underlying.remove(key)
    this
  }

  def iterator: Iterator[(String, String)] =
    underlying.iteratorAsString().asScala.map(entryToTuple)

}

private[http] object Netty4HeaderMap {
  val entryToTuple: (Entry[String, String]) => (String, String) =
    { entry: Entry[String, String] => entry.getKey -> entry.getValue }
}
