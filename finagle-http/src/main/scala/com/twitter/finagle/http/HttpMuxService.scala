package com.twitter.finagle.http

import com.twitter.finagle.Service
import com.twitter.util.Future
import java.net.URI
import org.jboss.netty.handler.codec.http.{
  DefaultHttpResponse, HttpHeaders, HttpRequest, HttpResponse, HttpResponseStatus, 
  HttpVersion}

/**
 * A service that dispatches incoming requests to registered handlers.
 * In order to choose which handler to dispatch the request to, we take the path of the request and match it with
 * the patterns of the pre-registered handlers. The pattern matching follows these rules:
 *  - Patterns ending with "/" use prefix matching. Eg: the pattern "foo/bar/" matches these paths:
 *            "foo/bar", "foo/bar/", "foo/bar/baz", etc.
 *  - Patterns not ending with "/" use exact matching. Eg: the pattern "foo/bar" ONLY matches these two paths:
 *            "foo/bar" and "foo/bar/"
 *  - Exact matching overrides prefix matching.
 *  - When multiple prefix matches exist, the longest pattern wins.
 */
class HttpMuxService(protected[this] val patterns: Seq[(String, Service[HttpRequest, HttpResponse])])
  extends Service[HttpRequest, HttpResponse] {

  def this() = this(Seq[(String, Service[HttpRequest, HttpResponse])]())

  private[this] val sorted: Seq[(String, Service[HttpRequest, HttpResponse])] =
    patterns.sortBy { case (pattern, _) => pattern.length } reverse

  /**
   * Create a new Mux service with the specified pattern added. If the pattern already exists, overwrite existing value.
   * Pattern ending with "/" indicates prefix matching; otherwise exact matching.
   */
  def withHandler(pattern: String, service: Service[HttpRequest, HttpResponse]): HttpMuxService = {
    val norm = normalize(pattern)
    new HttpMuxService(patterns.filterNot { case (pat, _) => pat == norm } :+ (norm, service))
  }

  /**
   * Extract path from HttpRequest; look for a matching pattern; if found, dispatch the
   * HttpRequest to the registered service; otherwise create a NOT_FOUND response
   */
  def apply(request: HttpRequest): Future[HttpResponse] = {
    val path = normalize(new URI(request.getUri()).getPath())

    // find the longest prefix of path; patterns are already sorted by length in descending order.
    val matching = sorted.find { case (pattern, _) =>
      (pattern.endsWith("/") && path.startsWith(pattern)) || // prefix
      (!pattern.endsWith("/") && path == pattern) // exact match
    }

    matching match {
      case Some((_, service)) => service(request)
      case None =>
        val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND)
        response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, 0.toString)
        Future.value(response)
    }
  }

  /**
   * - ensure path starts with "/"
   * - get rid of excessive "/"s. For example "/a//b///c/" => "/a/b/c/"
   */
  private[this] def normalize(path: String) = {
    val suffix = if (path.endsWith("/")) "/" else ""
    val p = path.split("/") filterNot(_.isEmpty) mkString "/"
    "/" + p + suffix
  }
}

/**
 * Singleton default multiplex service
 */
object DefaultHttpMuxService extends Service[HttpRequest, HttpResponse] {
  @volatile private[this] var underlying = new HttpMuxService()
  override def apply(request: HttpRequest): Future[HttpResponse] =
    underlying(request)

  /**
   * add handlers to mutate dispatching strategies.
   */
  def addHandler(pattern: String, service: Service[HttpRequest, HttpResponse]) = synchronized {
    underlying = underlying.withHandler(pattern, service)
  }
}
