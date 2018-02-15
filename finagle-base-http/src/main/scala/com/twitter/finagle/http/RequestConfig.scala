package com.twitter.finagle.http

import java.net.URL

/** Immutable representation of a [[Request]] used by `RequestBuilder` */
private[finagle] final case class RequestConfig(
  url: Option[URL] = None,
  headers: Map[String, Seq[String]] = Map.empty,
  formElements: Seq[FormElement] = Nil,
  version: Version = Version.Http11,
  proxied: Boolean = false
)

private[finagle] object RequestConfig {
  // absoluteURI if proxied, otherwise relativeURI
  def resource(config: RequestConfig): String = {
    val url = config.url.get
    if (config.proxied) {
      url.toString
    } else {
      val builder = new StringBuilder()

      val path = url.getPath
      if (path == null || path.isEmpty)
        builder.append("/")
      else
        builder.append(path)

      val query = url.getQuery
      if (query != null && !query.isEmpty)
        builder.append("?%s".format(query))

      builder.toString
    }
  }
}
