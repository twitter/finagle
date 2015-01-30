package com.twitter.finagle.http.service

import com.twitter.finagle.Service
import com.twitter.finagle.http.{Ask, Response}
import com.twitter.finagle.http.path.Path
import com.twitter.util.Future
import org.jboss.netty.handler.codec.http.HttpMethod


/**
 * RoutingService for composing Services.  Responds with 404 Not Found if no
 * matching service.
 *
 * RoutingService.byPath {
 *   case "/search.json" => mySearchService
 *   ....
 * }
 */
class RoutingService[ASK <: Ask](
     val routes: PartialFunction[Ask, Service[ASK, Response]])
  extends Service[ASK, Response] {

  // Try routes, fall back to 404 Not Found
  protected[this] val notFoundService = new NotFoundService[ASK]
  protected[this] val notFoundPf: PartialFunction[ASK, Service[ASK, Response]] = {
    case _ => notFoundService
  }
  protected[this] val requestToService = routes orElse notFoundPf

  def apply(request: ASK): Future[Response] = {
    val service = requestToService(request)
    service(request)
  }
}


object RoutingService {
  def byPath[ASK](routes: PartialFunction[String, Service[ASK, Response]]) =
   new RoutingService(
     new PartialFunction[Ask, Service[ASK, Response]] {
       def apply(request: Ask)       = routes(request.path)
       def isDefinedAt(request: Ask) = routes.isDefinedAt(request.path)
     })

  def byPathObject[ASK](routes: PartialFunction[Path, Service[ASK, Response]]) =
   new RoutingService(
     new PartialFunction[Ask, Service[ASK, Response]] {
       def apply(request: Ask)       = routes(Path(request.path))
       def isDefinedAt(request: Ask) = routes.isDefinedAt(Path(request.path))
     })

  def byMethodAndPath[ASK](routes: PartialFunction[(HttpMethod, String), Service[ASK, Response]]) =
    new RoutingService(
      new PartialFunction[Ask, Service[ASK, Response]] {
        def apply(request: Ask) = routes((request.method, request.path))
        def isDefinedAt(request: Ask) = routes.isDefinedAt((request.method, request.path))
      })

  def byMethodAndPathObject[ASK](routes: PartialFunction[(HttpMethod, Path), Service[ASK, Response]]) =
    new RoutingService(
      new PartialFunction[Ask, Service[ASK, Response]] {
        def apply(request: Ask) = routes((request.method, Path(request.path)))
        def isDefinedAt(request: Ask) = routes.isDefinedAt((request.method, Path(request.path)))
      })
}
