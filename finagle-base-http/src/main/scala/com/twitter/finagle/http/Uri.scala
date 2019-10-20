package com.twitter.finagle.http

import java.util.{Map => JMap, List => JList, Collections}
import scala.collection.JavaConverters._

object Uri {

  /**
   * Constructs a Uri from the Host header and path component of a Request.
   */
  def fromRequest(req: Request): Uri = {
    val uri = req.uri
    uri.indexOf('?') match {
      case -1 => new Uri(req.host, uri, None)
      case n => new Uri(req.host, uri.substring(0, n), Some(uri.substring(n + 1, uri.length)))
    }
  }
}

/**
 * Represents an immutable URI.
 */
final class Uri private (host: Option[String], val path: String, query: Option[String]) {

  def this(host: String, path: String, query: String) =
    this(Some(host), path, Some(query))

  def this(host: String, path: String) =
    this(Some(host), path, None)

  override def toString: String = {
    val prefix = host.getOrElse("")
    query match {
      case Some(q) => s"$prefix$path?$q"
      case None => s"$prefix$path"
    }
  }

  /**
   * Extracts the parameters from the query string and returns a ParamMap.
   */
  def params: ParamMap = _params
  private[this] val _params: ParamMap = {
    val decoded: JMap[String, JList[String]] = query match {
      case Some(q) => QueryParamDecoder.decodeParams(q)
      case None => Collections.emptyMap[String, JList[String]]
    }
    val map: Map[String, Seq[String]] = decoded.asScala.toMap.transform {
      case (_, v) => v.asScala.toSeq
    }
    new MapParamMap(map)
  }
}
