package com.twitter.finagle.http

import com.twitter.finagle.http.util.StringUtil
import java.util.{List => JList, Map => JMap}
import scala.jdk.CollectionConverters._

/**
 * Request parameter map.
 *
 * This is a persistent (immutable) multi-map.
 *
 * Use `getAll()` to get all values for a key.
 */
abstract class ParamMap extends ParamMapVersionSpecific {

  /**
   * Add a key/value pair to the map, returning a new map.
   * Overwrites all values if the key exists.
   */
  protected def setParam[B >: String](kv: (String, B)): ParamMap = {
    val (key, value) = (kv._1, kv._2.toString)
    val map = MapParamMap.tuplesToMultiMap(iterator.toSeq)
    val mapWithKey = map.updated(key, Seq(value))
    new MapParamMap(mapWithKey, isValid)
  }

  /**
   * Add a key/value pair to the map, returning a new map.
   * Overwrites all values if the key exists.
   */
  override def +[V1 >: String](kv: (String, V1)): ParamMap = setParam((kv._1, kv._2))

  /**
   * Removes a key from this map, returning a new map.
   * All values for the key are removed.
   */
  protected def clearParam(name: String): ParamMap = {
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
    get(name) match {
      case Some(v) => v
      case None => default
    }

  /** Get Short value.  Uses forgiving StringUtil.toSomeShort to parse. */
  def getShort(name: String): Option[Short] =
    get(name).map(ParamMap.ToShort)

  /** Get Short value or default.  Equivalent to getShort(name).getOrElse(default). */
  def getShortOrElse(name: String, default: => Short): Short =
    getShort(name) match {
      case Some(v) => v
      case None => default
    }

  /** Get Int value.  Uses forgiving StringUtil.toSomeInt to parse. */
  def getInt(name: String): Option[Int] =
    get(name).map(ParamMap.ToInt)

  /** Get Int value or default.  Equivalent to getInt(name).getOrElse(default). */
  def getIntOrElse(name: String, default: => Int): Int =
    getInt(name) match {
      case Some(v) => v
      case None => default
    }

  /** Get Long value.  Uses forgiving StringUtil.toLong to parse. */
  def getLong(name: String): Option[Long] =
    get(name).map(ParamMap.ToLong)

  /** Get Long value or default.  Equivalent to getLong(name).getOrElse(default). */
  def getLongOrElse(name: String, default: => Long): Long =
    getLong(name) match {
      case Some(v) => v
      case None => default
    }

  /** Get Boolean value.  Uses StringUtil.toBoolean to parse. */
  def getBoolean(name: String): Option[Boolean] =
    get(name).map(ParamMap.ToBoolean)

  /** Get Boolean value or default. Equivalent to getBoolean(name).getOrElse(default). */
  def getBooleanOrElse(name: String, default: => Boolean): Boolean =
    getBoolean(name) match {
      case Some(v) => v
      case None => default
    }

  override def toString: String = QueryParamEncoder.encode(this)
}

/** Map-backed ParamMap. */
class MapParamMap(underlying: Map[String, Seq[String]], val isValid: Boolean = true)
    extends ParamMap {

  def get(name: String): Option[String] =
    underlying.get(name) match {
      case Some(ss) => ss.headOption
      case None => None
    }

  def getAll(name: String): Iterable[String] =
    underlying.getOrElse(name, Nil)

  def iterator: Iterator[(String, String)] = {
    for ((k, vs) <- underlying.iterator; v <- vs) yield (k, v)
  }

  override def keySet: Set[String] =
    underlying.keySet

  override def keysIterator: Iterator[String] =
    underlying.keysIterator
}

object MapParamMap {
  def apply(params: (String, String)*): MapParamMap =
    new MapParamMap(MapParamMap.tuplesToMultiMap(params))

  def apply(map: Map[String, String]): MapParamMap =
    new MapParamMap(map.transform {
      case (_, value) =>
        Seq(value)
    })

  private[http] def tuplesToMultiMap(tuples: Seq[(String, String)]): Map[String, Seq[String]] = {
    tuples
      .groupBy { case (k, v) => k }
      .transform {
        case (_, values) =>
          values.map { _._2 }
      }
  }
}

/** Empty ParamMap */
object EmptyParamMap extends ParamMap {
  val isValid = true
  def get(name: String): Option[String] = None
  def getAll(name: String): Iterable[String] = Nil
  def iterator: Iterator[(String, String)] = Iterator.empty
  protected override def clearParam(name: String): ParamMap = this
  override def +[B >: String](kv: (String, B)): ParamMap = MapParamMap(kv._1 -> kv._2.toString)
}

/**
 * Http [[Request]]-backed [[ParamMap]]. This [[ParamMap]] contains both
 * parameters provided as part of the request URI and parameters provided
 * as part of the request body.
 *
 * @note Request body parameters are considered if the following criteria are true:
 *   1. The request is not a TRACE request.
 *   2. The request media type is 'application/x-www-form-urlencoded'
 *   3. The content length is greater than 0.
 */
class RequestParamMap(val request: Request) extends ParamMap {
  override def isValid: Boolean = _isValid

  private[this] var _isValid = true

  private[this] val getParams: JMap[String, JList[String]] =
    parseParams(request.uri)

  private[this] val postParams: JMap[String, JList[String]] = {
    if (request.method != Method.Trace &&
      request.mediaType.contains(MediaType.WwwForm) &&
      request.length > 0) {
      parseParams("?" + request.contentString)
    } else {
      ParamMap.EmptyJMap
    }
  }

  // Convert IllegalArgumentException to ParamMapException so it can be handled
  // appropriately (e.g., 400 Bad Request).
  private[this] def parseParams(s: String): JMap[String, JList[String]] = {
    try QueryParamDecoder.decode(s)
    catch {
      case _: IllegalArgumentException =>
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
      case None => jget(getParams, name)
      case value => value
    }

  def iterator: Iterator[(String, String)] =
    jiterator(postParams) ++ jiterator(getParams)

  override def keySet: Set[String] =
    postParams.keySet.asScala.toSet ++ getParams.keySet.asScala.toSet

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
      entry.getValue.asScala map { value => (entry.getKey, value) }
    }.toIterator
}

object ParamMap {

  /** Create ParamMap from parameter list. */
  def apply(params: (String, String)*): ParamMap =
    MapParamMap(params: _*)

  /** Create ParamMap from a map. */
  def apply(map: Map[String, String]): ParamMap =
    MapParamMap(map)

  private[http] val EmptyJMap = new java.util.HashMap[String, JList[String]]

  private val ToShort = { s: String => StringUtil.toSomeShort(s) }
  private val ToInt = { s: String => StringUtil.toSomeInt(s) }
  private val ToLong = { s: String => StringUtil.toSomeLong(s) }
  private val ToBoolean = { s: String => StringUtil.toBoolean(s) }
}
