package com.twitter.finagle.http.service

import com.twitter.finagle.Service
import com.twitter.finagle.http.{Request, Response, Method}
import com.twitter.finagle.http.path.Path
import com.twitter.util.Future

/**
 * RoutingService for composing Services.  Responds with 404 Not Found if no
 * matching service.
 *
 * RoutingService.byPath {
 *   case "/search.json" => mySearchService
 *   ....
 * }
 */
class RoutingService[REQUEST <: Request](
     val routes: PartialFunction[Request, Service[REQUEST, Response]])
  extends Service[REQUEST, Response] {

  // Try routes, fall back to 404 Not Found
  protected[this] val notFoundService = new NotFoundService[REQUEST]
  protected[this] val notFoundPf: PartialFunction[REQUEST, Service[REQUEST, Response]] = {
    case _ => notFoundService
  }
  protected[this] val requestToService = routes orElse notFoundPf

  def apply(request: REQUEST): Future[Response] = {
    val service = requestToService(request)
    service(request)
  }
}


object RoutingService {
  def byPath[REQUEST](routes: PartialFunction[String, Service[REQUEST, Response]]) =
   new RoutingService(
     new PartialFunction[Request, Service[REQUEST, Response]] {
       def apply(request: Request)       = routes(request.path)
       def isDefinedAt(request: Request) = routes.isDefinedAt(request.path)
     })

  def byPathObject[REQUEST](routes: PartialFunction[Path, Service[REQUEST, Response]]) =
   new RoutingService(
     new PartialFunction[Request, Service[REQUEST, Response]] {
       def apply(request: Request)       = routes(Path(request.path))
       def isDefinedAt(request: Request) = routes.isDefinedAt(Path(request.path))
     })

  def byMethodAndPath[REQUEST](routes: PartialFunction[(Method, String), Service[REQUEST, Response]]) =
    new RoutingService(
      new PartialFunction[Request, Service[REQUEST, Response]] {
        def apply(request: Request) = routes((request.method, request.path))
        def isDefinedAt(request: Request) = routes.isDefinedAt((request.method, request.path))
      })

  def byMethodAndPathObject[REQUEST](routes: PartialFunction[(Method, Path), Service[REQUEST, Response]]) =
    new RoutingService(
      new PartialFunction[Request, Service[REQUEST, Response]] {
        def apply(request: Request) = routes((request.method, Path(request.path)))
        def isDefinedAt(request: Request) = routes.isDefinedAt((request.method, Path(request.path)))
      })
}
