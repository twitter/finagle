package com.twitter.finagle.http

import com.twitter.finagle.util.LoadService
import com.twitter.finagle.Service
import com.twitter.util.{Closable, Future, Time}
import java.util.logging.Logger
import scala.annotation.tailrec

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
class HttpMuxer(_routes: Seq[Route]) extends Service[Request, Response] {
  import HttpMuxer._

  def this() = this(Seq.empty[Route])

  private[this] val sortedMatchers: Array[RouteMatcher] =
    _routes
      .sortBy(_.pattern.length)
      .reverse
      .map(new RouteMatcher(_))
      .toArray

  protected def routes: Seq[Route] = _routes

  def patterns: Seq[String] = sortedMatchers.map(_.route.pattern)

  /**
   * Create a new Mux service with the specified pattern added. If the pattern already exists, overwrite existing value.
   * Pattern ending with "/" indicates prefix matching; otherwise exact matching.
   */
  def withHandler(pattern: String, service: Service[Request, Response]): HttpMuxer = {
    withHandler(Route(pattern, service))
  }

  def withHandler(route: Route): HttpMuxer = {
    val norm = normalize(route.pattern)
    val newRoute = Route(pattern = norm, handler = route.handler, index = route.index)
    new HttpMuxer(routes.filterNot { route =>
      route.pattern == norm
    } :+ newRoute)
  }

  /**
   * Find the route that a request will take, or None if there's no matching
   * route.
   */
  def route(request: Request): Option[Route] = {
    val path = normalize(request.path)

    // find the longest pattern that matches (the patterns are already sorted)
    @tailrec
    def go(i: Int): Option[Route] = {
      if (i >= sortedMatchers.size) {
        None
      } else {
        val matcher = sortedMatchers(i)
        if (matcher.matches(path)) matcher.someRoute
        else go(i + 1)
      }
    }
    go(0)
  }

  /**
   * Extract path from Request; look for a matching pattern; if found, dispatch the
   * Request to the registered service; otherwise create a NOT_FOUND response
   */
  def apply(request: Request): Future[Response] = route(request) match {
    case Some(Route(_, handler, _)) => handler(request)
    case None => Future.value(Response(request.version, Status.NotFound))
  }

  override def close(deadline: Time): Future[Unit] =
    Closable.all(routes.map(_.handler): _*).close(deadline)
}

/**
 * Singleton default multiplex service.
 *
 * @see [[HttpMuxers]] for Java compatibility APIs.
 */
object HttpMuxer extends HttpMuxer {

  private final class RouteMatcher(val route: Route) {
    private[this] val isEmpty = route.pattern.isEmpty
    private[this] val usePrefixMatch = route.pattern.endsWith("/")
    val someRoute: Some[Route] = Some(route)

    def matches(path: String): Boolean = {
      if (isEmpty) {
        path == "/" || path.isEmpty
      } else {
        val pattern = route.pattern
        if (usePrefixMatch)
          path.startsWith(pattern)
        else
          path == pattern
      }
    }
  }

  @volatile private[this] var underlying = new HttpMuxer()

  private[this] def addLeadingAndStripDuplicateSlashes(s: String): String = {
    val b = new java.lang.StringBuilder(s.length)
    b.append('/')

    @tailrec
    def go(i: Int, lastSlash: Boolean): Unit = {
      if (i < s.length) {
        val c = s.charAt(i)
        if (lastSlash && c == '/') go(i + 1, true)
        else {
          b.append(c)
          go(i + 1, c == '/')
        }
      }
    }
    go(0, true)

    b.toString
  }

  /**
   * - ensure path starts with "/" (unless path is "")
   * - get rid of excessive "/"s. For example "/a//b///c/" => "/a/b/c/"
   * - return "" if path is ""
   * - return "/" if path is "/" or "///" etc
   *
   * @note exposed for testing.
   */
  private[http] def normalize(path: String): String = {
    if (path.isEmpty) {
      ""
    } else if (!path.contains("//")) {
      if (path.startsWith("/")) path
      else "/" + path
    } else {
      addLeadingAndStripDuplicateSlashes(path)
    }
  }

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

  private[this] val log = Logger.getLogger(getClass.getName)

  for (handler <- LoadService[HttpMuxHandler]()) {
    log.info(
      "HttpMuxer[%s] = %s(%s)".format(handler.route.pattern, handler.getClass.getName, handler)
    )
    addHandler(handler.route)
  }

  override def apply(req: Request): Future[Response] = underlying(req)

  override def route(req: Request): Option[Route] = underlying.route(req)

  override def routes: Seq[Route] = underlying.routes

  override def patterns: Seq[String] = underlying.patterns

  override def withHandler(route: Route): HttpMuxer = underlying.withHandler(route)

  override def close(deadline: Time): Future[Unit] = underlying.close(deadline)
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

  /** Configures the route for this handler */
  def route: Route
}
