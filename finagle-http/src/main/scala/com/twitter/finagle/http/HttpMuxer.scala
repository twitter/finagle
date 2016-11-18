package com.twitter.finagle.http

import com.twitter.finagle.util.LoadService
import com.twitter.finagle.Service
import com.twitter.util.Future
import java.util.logging.Logger

/**
 * A service that dispatches incoming requests to registered handlers.
 * In order to choose which handler to dispatch the request to, we take the path of the request and match it with
 * the patterns of the pre-registered handlers. The pattern matching follows these rules:
 *
 *  - Patterns ending with "/" use exclusive prefix matching. Eg: the pattern "foo/bar/" matches these paths:
 *            "foo/bar/", "foo/bar/baz", etc but NOT "foo/bar"
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
class HttpMuxer(protected val routes: Seq[Route])
  extends Service[Request, Response] {

  def this() = this(Seq.empty[Route])

  private[this] val sorted: Seq[Route] =
    routes.sortBy(_.pattern.length).reverse

  def patterns: Seq[String] = sorted.map(_.pattern)

  /**
   * Create a new Mux service with the specified pattern added. If the pattern already exists, overwrite existing value.
   * Pattern ending with "/" indicates prefix matching; otherwise exact matching.
   */
  def withHandler(pattern: String, service: Service[Request, Response]): HttpMuxer = {
    withHandler(Route(pattern, service))
  }

  def withHandler(route: Route): HttpMuxer = {
    val norm = normalize(route.pattern)
    val newRoute = Route(
      pattern = norm,
      handler = route.handler,
      index = route.index)
    new HttpMuxer(routes.filterNot { route => route.pattern == norm} :+ newRoute)
  }

  /**
   * Extract path from Request; look for a matching pattern; if found, dispatch the
   * Request to the registered service; otherwise create a NOT_FOUND response
   */
  def apply(request: Request): Future[Response] = {
    val path = normalize(request.path)

    // find the longest pattern that matches (the patterns are already sorted)
    val matching = sorted.find { route =>
      val pattern = route.pattern
      if (pattern == "")
        path == "/" || path == "" // special cases
      else if (pattern.endsWith("/"))
        path.startsWith(pattern) // prefix match
      else
        path == pattern // exact match
    }

    matching match {
      case Some(Route(_, handler, _)) => handler(request)
      case None => Future.value(Response(request.version, Status.NotFound))
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
    val p = path.split("/").filterNot(_.isEmpty).mkString("/")
    if (p == "") suffix else "/" + p + suffix
  }
}

/**
 * Singleton default multiplex service.
 *
 * @see [[HttpMuxers]] for Java compatibility APIs.
 */
object HttpMuxer extends Service[Request, Response] {
  @volatile private[this] var underlying = new HttpMuxer()

  override def apply(request: Request): Future[Response] =
    underlying(request)

  /**
   * add handlers to mutate dispatching strategies.
   */
  def addHandler(route: Route): Unit = synchronized {
    underlying = underlying.withHandler(route)
  }

  /**
   * @see [[addHandler(Route)]] for an updated version
   */
  def addHandler(pattern: String, service: Service[Request, Response]): Unit = {
    addHandler(Route(pattern = pattern, handler = service))
  }

  def addRichHandler(pattern: String, service: Service[Request, Response]): Unit =
    addHandler(Route(pattern = pattern, handler = service))

  def patterns: Seq[String] = underlying.patterns
  def routes: Seq[Route] = underlying.routes

  private[this] val log = Logger.getLogger(getClass.getName)

  for (handler <- LoadService[HttpMuxHandler]()) {
    log.info("HttpMuxer[%s] = %s(%s)".format(handler.pattern, handler.getClass.getName, handler))
    addHandler(handler.route)
  }
}

/**
 * Java compatibility APIs for [[HttpMuxer]].
 */
object HttpMuxers {

  /** See [[HttpMuxer.apply]] */
  def apply(request: Request): Future[Response] = HttpMuxer(request)

  /** See [[HttpMuxer.patterns]] */
  def patterns: Seq[String] = HttpMuxer.patterns

}

/**
 * Trait HttpMuxHandler is used for service-loading HTTP handlers.
 */
trait HttpMuxHandler extends Service[Request, Response] {
  /** The pattern that this handler gets bound to */
  @deprecated("Use route(pattern, this) instead", "2016-11-08")
  val pattern: String

  /** Configures the route for this handler */
  def route: Route = Route(pattern, this)
}
