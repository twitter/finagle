package com.twitter.finagle.http.exp.routing

import com.twitter.finagle.exp.routing.{Request => Req}
import com.twitter.finagle.http.exp.routing.Fields.PathField
import com.twitter.finagle.http.{Method, Request}
import com.twitter.finagle.http.exp.routing.HttpRouter.{
  PathRouter,
  PathRouterGenerator,
  RoutesForPath
}
import com.twitter.util.{Future, Time}
import com.twitter.util.routing.{
  Found,
  Generator,
  NotFound,
  Router,
  RouterBuilder,
  RouterInfo,
  Result => RouteResult
}
import java.lang.IllegalStateException

private[http] object HttpRouter {

  /**
   * Represents the [[Route routes]] and their [[com.twitter.finagle.http.Method methods]]
   * that correspond to a given [[Path]].
   *
   * @param path The [[Path]] for the given [[Route routes]].
   * @param routes A map from a [[Method]] to a [[Route]] for the associated [[Path]].
   */
  final case class RoutesForPath(path: Path, routes: Map[Method, Route]) {
    def allowedMethods: Iterable[Method] = routes.keys
  }

  /**
   * A [[Router]] implementation that finds [[RoutesForPath]] for an input request.
   * A [[PathRouter]] that is spec compliant should ensure that a matching [[Path]]
   * that [[Path.isConstant is constant]] is returned as the result BEFORE a matching [[Path]]
   * that contains [[Parameter parameters]].
   *
   * @see [[https://swagger.io/specification/#paths-object OpenAPI Path matching behavior]]
   */
  sealed abstract class PathRouter(label: String, routes: Iterable[RoutesForPath])
      extends Router[Req[Request], RoutesForPath](label, routes) {

    /** Extract the memoized path String from the input request */
    protected final def getPath(req: Req[Request]): String = req.get(PathField) match {
      case Some(path) => path
      case _ => throw new IllegalStateException("no path on request")
    }

  }

  /** Contract for a [[Generator]] that will produce a new [[PathRouter]] */
  sealed abstract class PathRouterGenerator
      extends Generator[Req[Request], RoutesForPath, PathRouter]

  /**
   * A [[Generator]] that will produce a new [[HttpRouter]] which will use the
   * resulting [[PathRouter]] from [[PathRouterGenerator]] for determining matching [[Path paths]].
   */
  final class HttpRouterGenerator(pathRouterGenerator: PathRouterGenerator)
      extends Generator[Req[Request], Route, HttpRouter] {
    def apply(routeInfo: RouterInfo[Route]): HttpRouter =
      new HttpRouter(routeInfo.label, routeInfo.routes, pathRouterGenerator)
  }

  /** Create a new [[RouterBuilder]] for defining HTTP [[Route routes]]. */
  def newBuilder(
    pathRouterGenerator: PathRouterGenerator
  ): RouterBuilder[Req[Request], Route, HttpRouter] =
    RouterBuilder.newBuilder[Req[Request], Route, HttpRouter](
      new HttpRouterGenerator(pathRouterGenerator))

}

/** A [[Router]] that works with our HTTP [[Request]] and [[Route]] types. */
private[routing] final class HttpRouter(
  label: String,
  routes: Iterable[Route],
  pathRouterGenerator: Generator[Req[Request], RoutesForPath, PathRouter])
    extends Router[Req[Request], Route](label, routes) {

  // transform our `routes` into grouped Routes for a path that our `pathRouter` can use
  private[this] def groupRoutesByPath(): Iterable[RoutesForPath] = {
    routes
      .groupBy(_.schema.path)
      .map {
        case (path: Path, rfp: Iterable[Route]) =>
          val methodMap = rfp.map(r => r.schema.method -> r).toMap
          require(
            rfp.size == methodMap.size,
            s"Path '$path' cannot have multiple routes for the " +
              s"same Method: ${duplicateMethodString(rfp)}")
          RoutesForPath(path, methodMap)
      }
  }

  // generate a message that tries to make it easier to show what Paths are duplicates
  private[this] def duplicateMethodString(routes: Iterable[Route]): String = routes
    .map { route =>
      s"${route.schema.method} (${route.label})"
    }.mkString("[", ",", "]")

  // This underlying PathRouter will do our Path matching logic and find associated RoutesForPath
  private[this] val pathRouter: PathRouter = pathRouterGenerator(
    RouterInfo(label, groupRoutesByPath()))

  protected def find(input: Req[Request]): RouteResult = {
    val populatedReq = input.set(PathField, input.value.path)
    pathRouter(populatedReq) match {
      case Found(req: Req[_], rfp: RoutesForPath) =>
        // if we have found RoutesForPath that match our request, we need to ensure
        // that the request's Method is defined in the RoutesForPath in order to
        // return our single Route as a Found result. otherwise, we need to signal that
        // the Method is not allowed.
        val method = req.asInstanceOf[Req[Request]].value.method
        rfp.routes.get(method) match {
          case Some(route) =>
            Found(req, route)
          case _ =>
            // we throw here because a result that is NotFound maps to an HTTP 404 status,
            // which is not the same as MethodNotAllowed HTTP 405 status.
            throw MethodNotAllowedException(this, method, rfp.allowedMethods)
        }
      case Found(_, _) =>
        // we should never encounter this, but let's be defensive
        throw new IllegalStateException(
          "Wrong Result value type returned, was expecting RoutesForPath")
      case status =>
        // we forward any other result status
        status
    }

  }

  // we override so that we can clean-up our resources
  override protected def closeOnce(deadline: Time): Future[Unit] = Future.join(
    Seq(
      super.closeOnce(deadline),
      pathRouter.close(deadline)
    )
  )
}

/**
 * A fully compliant [[PathRouter]] that works in O(n) linear time to determine matches.
 * The worst case performance for this [[PathRouter]] is when a [[NotFound]] [[RouteResult result]]
 * is determined, as every [[RoutesForPath path]] will have been compared against an input request.
 */
private[routing] final class LinearPathRouter(
  label: String,
  routes: Iterable[RoutesForPath])
    extends PathRouter(label, routes) {

  // we need to split the routes by Constant and Parameterized Paths, because
  // we need to attempt to match Constant Paths before Parameterized Paths
  private[this] val (
    constantRouter: LinearPathRouter.ConstantRouter,
    parameterizedRouter: LinearPathRouter.ParameterizedRouter) = {
    val (constant, parameterized) = routes.partition(_.path.isConstant)
    (
      LinearPathRouter.ConstantRouterGenerator(RouterInfo(label, constant)),
      LinearPathRouter.ParameterizedRouterGenerator(RouterInfo(label, parameterized)))
  }

  protected def find(input: Req[Request]): RouteResult = {
    // attempt to match constant routes first
    constantRouter(input) match {
      case route @ Found(_, _) =>
        // return the constant route match if it's found
        route
      case NotFound =>
        // if it wasn't found, we return the result of searching the parameterized routes
        parameterizedRouter(input)
      case status =>
        // we forward any other result status
        status
    }
  }

  // we override so that we can clean-up our resources
  override protected def closeOnce(deadline: Time): Future[Unit] = Future.join(
    Seq(
      super.closeOnce(deadline),
      constantRouter.close(deadline),
      parameterizedRouter.close(deadline)
    )
  )

}

private[http] object LinearPathRouter {

  /** A [[PathRouterGenerator]] for the [[LinearPathRouter]] */
  object Generator extends PathRouterGenerator {
    def apply(routerInfo: RouterInfo[RoutesForPath]): LinearPathRouter =
      new LinearPathRouter(routerInfo.label, routerInfo.routes)
  }

  /** A [[PathRouterGenerator]] for the [[ConstantRouter]] */
  private object ConstantRouterGenerator extends PathRouterGenerator {
    def apply(routerInfo: RouterInfo[RoutesForPath]): ConstantRouter =
      new ConstantRouter(routerInfo.label, routerInfo.routes)
  }

  /**
   * A [[PathRouter]] that will only search a [[Path]] that contains NO [[Parameter parameters]].
   * The [[RouteResult]] will only be [[Found]] if there is a [[ConstantPathMatch]].
   *
   * @note This is not a fully compliant [[PathRouter]] and should only be used
   *       via [[LinearPathRouter]], NEVER directly.
   */
  private final class ConstantRouter(label: String, routes: Iterable[RoutesForPath])
      extends PathRouter(label + "-constant", routes) {
    protected def find(input: Req[Request]): RouteResult = {
      val path: String = getPath(input)
      val iter = routes.iterator
      var result: RouteResult = NotFound
      while (result == NotFound && iter.hasNext) {
        val routesForPath = iter.next()
        LinearPathMatcher(routesForPath.path, path) match {
          case ConstantPathMatch =>
            result = Found(input, routesForPath)
          case _ =>
            ()
        }
      }
      result
    }
  }

  /** A [[PathRouterGenerator]] for the [[ParameterizedRouter]] */
  private object ParameterizedRouterGenerator extends PathRouterGenerator {
    def apply(routerInfo: RouterInfo[RoutesForPath]): ParameterizedRouter =
      new ParameterizedRouter(routerInfo.label, routerInfo.routes)
  }

  /**
   * A [[PathRouter]] that will only search a [[Path]] that contains [[Parameter parameters]]
   * and must extract and return a [[ParameterMap]] as part of the [[RouteResult]].
   *
   * @note This is not a fully compliant [[PathRouter]] and should only be used
   *       via [[LinearPathRouter]], NEVER directly.
   */
  private final class ParameterizedRouter(label: String, routes: Iterable[RoutesForPath])
      extends PathRouter(label + "-parameterized", routes) {
    protected def find(input: Req[Request]): RouteResult = {
      val path: String = getPath(input)
      val iter = routes.iterator
      var result: RouteResult = NotFound
      while (result == NotFound && iter.hasNext) {
        val routesForPath = iter.next()
        LinearPathMatcher(routesForPath.path, path) match {
          case ParameterizedPathMatch(parameters) =>
            result = Found(input.set(Fields.ParameterMapField, parameters), routesForPath)
          case _ =>
            ()
        }
      }
      result
    }
  }
}

// TODO - TrieHttpRouter
