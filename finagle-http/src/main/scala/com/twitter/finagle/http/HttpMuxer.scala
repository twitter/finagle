package com.twitter.finagle.http

import com.twitter.finagle.util.LoadService
import com.twitter.finagle.{Filter, Service}
import com.twitter.util.Future
import java.net.URI
import java.util.logging.Logger
import org.jboss.netty.handler.codec.http.{DefaultHttpResponse, HttpHeaders,
  HttpRequest, HttpResponse, HttpResponseStatus, HttpVersion}

/**
 * A service that dispatches incoming requests to registered handlers.
 * In order to choose which handler to dispatch the request to, we take the path of the request and match it with
 * the patterns of the pre-registered handlers. The pattern matching follows these rules:
 *
 *  - Patterns ending with "/" use prefix matching. Eg: the pattern "foo/bar/" matches these paths:
 *            "foo/bar", "foo/bar/", "foo/bar/baz", etc.
 *    Similarly, the pattern "/" matches all paths
 *
 *  - Patterns not ending with "/" use exact matching. Eg: the pattern "foo/bar" ONLY matches this path:
 *            "foo/bar"
 *
 *  - Special case:
 *      The pattern "" matches only "/" and ""
 *
 *  NOTE: When multiple pattern matches exist, the longest pattern wins.
 */
class HttpMuxer(protected[this] val handlers: Seq[(String, Service[HttpRequest, HttpResponse])])
  extends Service[HttpRequest, HttpResponse] {

  def this() = this(Seq[(String, Service[HttpRequest, HttpResponse])]())

  private[this] val sorted: Seq[(String, Service[HttpRequest, HttpResponse])] =
    handlers.sortBy { case (pattern, _) => pattern.length } reverse

  def patterns = sorted map { case(p, _) => p }

  /**
   * Create a new Mux service with the specified pattern added. If the pattern already exists, overwrite existing value.
   * Pattern ending with "/" indicates prefix matching; otherwise exact matching.
   */
  def withHandler(pattern: String, service: Service[HttpRequest, HttpResponse]): HttpMuxer = {
    val norm = normalize(pattern)
    new HttpMuxer(handlers.filterNot { case (pat, _) => pat == norm } :+ (norm, service))
  }

  /**
   * Extract path from HttpRequest; look for a matching pattern; if found, dispatch the
   * HttpRequest to the registered service; otherwise create a NOT_FOUND response
   */
  def apply(request: HttpRequest): Future[HttpResponse] = {
    val u = request.getUri
    val uri = u.indexOf('?') match {
      case -1 => u
      case n  => u.substring(0, n)
    }
    val path = normalize(new URI(uri).getPath)

    // find the longest pattern that matches (the patterns are already sorted)
    val matching = sorted.find { case (pattern, _) =>
      if (pattern == "")
        path == "/" || path == "" // special cases
      else if (pattern.endsWith("/"))
        path.startsWith(pattern) || path == pattern.dropRight(1) // prefix match
      else
        path == pattern // exact match
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
   * - return "" if path is ""
   * - return "/" if path is "/" or "///" etc
   */
  private[this] def normalize(path: String) = {
    val suffix = if (path.endsWith("/")) "/" else ""
    val p = path.split("/") filterNot(_.isEmpty) mkString "/"
    if (p == "") suffix else "/" + p + suffix
  }
}

/**
 * Singleton default multiplex service
 */
object HttpMuxer extends Service[HttpRequest, HttpResponse] {
  @volatile private[this] var underlying = new HttpMuxer()
  override def apply(request: HttpRequest): Future[HttpResponse] =
    underlying(request)

  /**
   * add handlers to mutate dispatching strategies.
   */
  def addHandler(pattern: String, service: Service[HttpRequest, HttpResponse]) = synchronized {
    underlying = underlying.withHandler(pattern, service)
  }

  private[this] val nettyToFinagle =
    Filter.mk[HttpRequest, HttpResponse, Request, Response] { (req, service) =>
      service(Request(req)) map { _.httpResponse }
    }

  def addRichHandler(pattern: String, service: Service[Request, Response]) =
    addHandler(pattern, nettyToFinagle andThen service)

  def patterns = underlying.patterns

  private[this] val log = Logger.getLogger(getClass.getName)

  for (handler <- LoadService[HttpMuxHandler]()) {
    log.info("HttpMuxer[%s] = %s(%s)".format(handler.pattern, handler.getClass.getName, handler))
    addHandler(handler.pattern, handler)
  }
}

/**
 * Trait HttpMuxHandler is used for service-loading HTTP handlers.
 */
trait HttpMuxHandler extends Service[HttpRequest, HttpResponse] {
  /** The pattern on to bind this handler to */
  val pattern: String
}
