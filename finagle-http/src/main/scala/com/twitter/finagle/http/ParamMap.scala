package com.twitter.finagle.http

import com.twitter.finagle.http.util.StringUtil
import java.nio.charset.Charset
import java.util.{List => JList, Map => JMap}
import org.jboss.netty.handler.codec.http.{QueryStringDecoder, QueryStringEncoder}
import scala.collection.immutable
import scala.collection.JavaConverters._

/**
 * Request parameter map.
 *
 * This is a multi-map.  Use getAll() get all values for a key.
 */
abstract class ParamMap
  extends immutable.Map[String, String]
  with immutable.MapLike[String, String, ParamMap] {

  /**
   * Add a key/value pair to the map, returning a new map.
   * Overwrites all values if the key exists.
   */
  def +[B >: String](kv: (String, B)): ParamMap = {
    val (key, value) = (kv._1, kv._2.toString)
    val map = MapParamMap.tuplesToMultiMap(iterator.toSeq)
    val mapWithKey = map.updated(key, Seq(value))
    new MapParamMap(mapWithKey, isValid)
  }

  /**
   * Removes a key from this map, returning a new map.
   * All values for the key are removed.
   */
  def -(name: String): ParamMap = {
    val map = MapParamMap.tuplesToMultiMap(iterator.toSeq)
    new MapParamMap(map - name, isValid)
  }

  // For Map/MapLike
  override def empty: ParamMap =
    EmptyParamMap

  // For Map/MapLike (ensures keys aren't repeated)
  override def keySet: Set[String] =
    // super.keySet can actually have the same element multiple times
    super.keySet.toSeq.distinct.toSet

  // For Map/MapLike (ensures keys aren't repeated)
  override def keysIterator: Iterator[String] =
    super.keysIterator.toSeq.distinct.iterator

  /**
   * Check if there was a parse error.  On a parse error, the parameters
   * are treated as empty (versus throwing a parse exception).
   */
  def isValid: Boolean

  /** Get all parameters with name. */
  def getAll(name: String): Iterable[String]

  /* Equivalent to get(name).getOrElse(default). */
  def getOrElse(name: String, default: => String): String =
    get(name).getOrElse(default)

  /** Get Short value.  Uses forgiving StringUtil.toSomeShort to parse. */
  def getShort(name: String): Option[Short] =
    get(name) map { StringUtil.toSomeShort(_) }

  /** Get Short value or default.  Equivalent to getShort(name).getOrElse(default). */
  def getShortOrElse(name: String, default: => Short): Short =
    getShort(name) getOrElse default

  /** Get Int value.  Uses forgiving StringUtil.toSomeInt to parse. */
  def getInt(name: String): Option[Int] =
    get(name) map { StringUtil.toSomeInt(_) }

  /** Get Int value or default.  Equivalent to getInt(name).getOrElse(default). */
  def getIntOrElse(name: String, default: => Int): Int =
    getInt(name) getOrElse default

  /** Get Long value.  Uses forgiving StringUtil.toLong to parse. */
  def getLong(name: String): Option[Long] =
    get(name) map { StringUtil.toSomeLong(_) }

  /** Get Long value or default.  Equivalent to getLong(name).getOrElse(default). */
  def getLongOrElse(name: String, default: => Long): Long =
    getLong(name) getOrElse default

  /** Get Boolean value.  True is "1" or "true", false is all other values. */
  def getBoolean(name: String): Option[Boolean] =
    get(name) map { _.toLowerCase } map { v => v == "1" || v == "t" || v == "true" }

  /** Get Boolean value or default. Equivalent to getBoolean(name).getOrElse(default). */
  def getBooleanOrElse(name: String, default: => Boolean): Boolean =
    getBoolean(name) getOrElse default

  override def toString = {
    val encoder = new QueryStringEncoder("", Charset.forName("utf-8"))
    iterator foreach { case (k, v) =>
      encoder.addParam(k, v)
    }
    encoder.toString
  }
}


/** Map-backed ParamMap. */
class MapParamMap(
    underlying: Map[String, Seq[String]],
    val isValid: Boolean = true)
  extends ParamMap {

  def get(name: String): Option[String] =
    underlying.get(name) flatMap { _.headOption }

  def getAll(name: String): Iterable[String] =
    underlying.getOrElse(name, Nil)

  def iterator: Iterator[(String, String)] = {
    for ((k, vs) <- underlying.iterator; v <- vs) yield
      (k, v)
  }

  override def keySet: Set[String] =
    underlying.keySet

  override def keysIterator: Iterator[String] =
    underlying.keysIterator
}


object MapParamMap {
  def apply(params: Tuple2[String, String]*): MapParamMap =
    new MapParamMap(MapParamMap.tuplesToMultiMap(params))

  def apply(map: Map[String, String]): MapParamMap =
    new MapParamMap(map.mapValues { value => Seq(value) })

  private[http] def tuplesToMultiMap(
    tuples: Seq[Tuple2[String, String]]
  ): Map[String, Seq[String]] = {
    tuples
      .groupBy { case (k, v) => k }
      .mapValues { case values => values.map { _._2 } }
  }
}


/** Empty ParamMap */
object EmptyParamMap extends ParamMap {
  val isValid = true
  def get(name: String): Option[String] = None
  def getAll(name: String): Iterable[String] = Nil
  def iterator: Iterator[(String, String)] = Iterator.empty
  override def -(name: String): ParamMap = this
}

/**
 * HttpRequest-backed param map.  Handle parameters in the URL and form encoded
 * body.  Multipart forms are not supported (not needed, could be abusive).
 *
 * This map is a multi-map.  Use getAll() to get all values for a key.
 */
class RequestParamMap(val request: Request) extends ParamMap {
  override def isValid: Boolean = _isValid

  private[this] var _isValid = true

  private[this] val getParams: JMap[String, JList[String]] =
    parseParams(request.uri)

  private[this] val postParams: JMap[String, JList[String]] = {
    if (request.method != Method.Trace &&
        request.mediaType == Some(MediaType.WwwForm) &&
        request.length > 0) {
      parseParams("?" + request.contentString)
    } else {
      ParamMap.EmptyJMap
    }
  }

  // Convert IllegalArgumentException to ParamMapException so it can be handled
  // appropriately (e.g., 400 Bad Request).
  private[this] def parseParams(s: String): JMap[String, JList[String]] = {
    try {
      new QueryStringDecoder(s).getParameters
    } catch {
      case e: IllegalArgumentException =>
        _isValid = false
        ParamMap.EmptyJMap
    }
  }

  override def getAll(name: String): Iterable[String] =
    jgetAll(postParams, name) ++ jgetAll(getParams, name)

  // Map/MapLike interface

  /** Get value */
  def get(name: String): Option[String] =
    jget(postParams, name) match {
      case None  => jget(getParams, name)
      case value => value
    }

  def iterator: Iterator[(String, String)] =
    jiterator(postParams) ++ jiterator(getParams)

  override def keySet: Set[String] =
    (postParams.keySet.asScala.toSet ++ getParams.keySet.asScala.toSet)

  override def keysIterator: Iterator[String] =
    keySet.iterator

  // Get value from JMap, which might be null
  private def jget(params: JMap[String, JList[String]], name: String): Option[String] = {
    val values = params.get(name)
    if (values != null && !values.isEmpty()) {
      Some(values.get(0))
    } else {
      None
    }
  }

  // Get values from JMap, which might be null
  private def jgetAll(params: JMap[String, JList[String]], name: String): Iterable[String] = {
    val values = params.get(name)
    if (values != null) {
      values.asScala
    } else {
      Nil
    }
  }

  // Get iterable for JMap, which might be null
  private def jiterator(params: JMap[String, JList[String]]): Iterator[(String, String)] =
    params.entrySet.asScala.flatMap { entry =>
      entry.getValue.asScala map { value =>
        (entry.getKey, value)
      }
    }.toIterator
}


object ParamMap {
  /** Create ParamMap from parameter list. */
  def apply(params: Tuple2[String, String]*): ParamMap =
    MapParamMap(params: _*)

  /** Create ParamMap from a map. */
  def apply(map: Map[String, String]): ParamMap =
    MapParamMap(map)

  private[http] val EmptyJMap = new java.util.HashMap[String, JList[String]]
}
