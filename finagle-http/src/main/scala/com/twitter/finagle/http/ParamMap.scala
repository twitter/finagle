package com.twitter.finagle.http

import com.twitter.finagle.http.util.StringUtil
import java.nio.charset.Charset
import java.util.{List => JList, Map => JMap}
import org.jboss.netty.handler.codec.http.{QueryStringDecoder, QueryStringEncoder}
import scala.collection.MapLike
import scala.collection.JavaConversions._


/**
 * Adapt params of a Request to a mutable Map.  Handle parameters in the
 * URL and form encoded body.  Multipart forms are not supported (not
 * needed, could be abusive).
 */
class ParamMap(val request: Request)
  extends MapLike[String, String, Map[String, String]] {

  private[this] var _isValid = true

  private[this] val getParams: JMap[String, JList[String]] =
    parseParams(request.uri)

  private[this] val postParams: JMap[String, JList[String]] =
    if (request.method == Method.Post &&
        request.mediaType == Some(Message.MediaTypeWwwForm) &&
        request.length > 0)
      parseParams("?" + request.contentString)
    else
      // Most requests won't have body - don't bother creating an object for
      // the lifetime of the request.
      null

  // Convert IllegalArgumentException to ParamMapException so it can be handled
  // appropriately (e.g., 400 Bad Request).
  private[this] def parseParams(s: String): JMap[String, JList[String]] =
    try
      new QueryStringDecoder(s).getParameters
    catch {
      case e: IllegalArgumentException =>
        _isValid = false
        null
    }

  /**
   * Check if there was a parse error.  On a parse error, the parameters
   * are treated as empty (versus throwing a parse exception).
   */
  def isValid = _isValid

  /** Get value */
  def get(name: String): Option[String] =
    jget(postParams, name) match {
      case None  => jget(getParams, name)
      case value => value
    }

  /* Equivalent to get(name).getOrElse(default). */
  def getOrElse(name: String, default: String): String =
    get(name).getOrElse(default)

  /** Get Short value.  Uses forgiving StringUtil.toSomeShort to parse. */
  def getShort(name: String): Option[Short] =
    get(name) map { StringUtil.toSomeShort(_) }

  /** Get Short value or default.  Equivalent to getShort(name).getOrElse(default). */
  def getShortOrElse(name: String, default: Short): Short =
    getShort(name) getOrElse default

  /** Get Int value.  Uses forgiving StringUtil.toSomeInt to parse. */
  def getInt(name: String): Option[Int] =
    get(name) map { StringUtil.toSomeInt(_) }

  /** Get Int value or default.  Equivalent to getInt(name).getOrElse(default). */
  def getIntOrElse(name: String, default: Int): Int =
    getInt(name) getOrElse default

  /** Get Long value.  Uses forgiving StringUtil.toLong to parse. */
  def getLong(name: String): Option[Long] =
    get(name) map { StringUtil.toSomeLong(_) }

  /** Get Long value or default.  Equivalent to getLong(name).getOrElse(default). */
  def getLongOrElse(name: String, default: Long): Long =
    getLong(name) getOrElse default

  /** Get Boolean value.  True is "1" or "true", false is all other values. */
  def getBoolean(name: String): Option[Boolean] =
    get(name) map { _.toLowerCase } map { v => v == "1" || v == "t" || v == "true" }

  /** Get Boolean value or default. Equivalent to getBoolean(name).getOrElse(default). */
  def getBooleanOrElse(name: String, default: Boolean): Boolean =
    getBoolean(name) getOrElse default

  def getAll(name: String): Iterable[String] =
    jgetAll(postParams, name) ++ jgetAll(getParams, name)

  def iterator: Iterator[(String, String)] =
    jiterator(postParams) ++ jiterator(getParams)

  def +[B >: String](kv: (String, B)): Map[String, B] =
    Map.empty ++ iterator + kv

  def -(name: String): Map[String, String] =
    Map.empty ++ iterator - name

  def empty = Map.empty[String, String]

  override def toString = {
    val encoder = new QueryStringEncoder("", Charset.forName("utf-8"))
    iterator foreach { case (k, v) =>
      encoder.addParam(k, v)
    }
    encoder.toString
  }

  // Get value from JMap, which might be null
  private def jget(params: JMap[String, JList[String]], name: String): Option[String] = {
    if (params != null) {
      Option(params.get(name)) flatMap { _.headOption }
    } else {
      None
    }
  }

  // Get values from JMap, which might be null
  private def jgetAll(params: JMap[String, JList[String]], name: String): Iterable[String] = {
    if (params != null) {
      Option(params.get(name).toList) getOrElse Nil
    } else {
      None
    }
  }

  // Get iterable for JMap, which might be null
  private def jiterator(params: JMap[String, JList[String]]): Iterator[(String, String)] = {
    if (params != null) {
      params.entrySet flatMap { entry =>
        entry.getValue.toList map { value =>
          (entry.getKey, value)
        }
      } toIterator
    } else {
      Iterator.empty
    }
  }
}


object ParamMap {
  /** Create ParamMap from parameter list.  Convenience method for testing. */
  def apply(params: Tuple2[String, String]*): ParamMap =
    new ParamMap(Request(params:_*))
}
