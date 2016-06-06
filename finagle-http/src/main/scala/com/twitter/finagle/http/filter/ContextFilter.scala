package com.twitter.finagle.http.filter

import com.twitter.finagle._
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.http.codec.HttpContext
import com.twitter.util.Future

/**
 * Sets the following Context values from the request headers:
 *     - request deadline
 */
private[finagle] class ServerContextFilter[Req <: Request, Rep]
  extends SimpleFilter[Req, Rep] {

  def apply(req: Req, service: Service[Req, Rep]): Future[Rep] =
    HttpContext.read(req)(service(req))
}

private[finagle] object ServerContextFilter {
  val role = Stack.Role("ServerContext")

  val module: Stackable[ServiceFactory[Request, Response]] =
    new Stack.Module0[ServiceFactory[Request, Response]] {
      val role = ServerContextFilter.role
      val description = "Extract context information from requests"

      val dtab = new DtabFilter.Finagle[Request]
      val context = new ServerContextFilter[Request, Response]

      def make(next: ServiceFactory[Request, Response]) =
        dtab.andThen(context).andThen(next)
    }
}

/**
 * Sets the following header values for the request Context:
 *     - request deadline
 */
private[finagle] class ClientContextFilter[Req <: Request, Rep]
  extends SimpleFilter[Req, Rep] {

  def apply(req: Req, service: Service[Req, Rep]): Future[Rep] = {
    HttpContext.write(req)
    service(req)
  }
}

private[finagle] object ClientContextFilter {
  val role = Stack.Role("ClientContext")

  val module: Stackable[ServiceFactory[Request, Response]] =
    new Stack.Module0[ServiceFactory[Request, Response]] {
      val role = ClientContextFilter.role
      val description = "Set context information on client requests"
      def make(next: ServiceFactory[Request, Response]) =
        new ClientContextFilter[Request, Response].andThen(next)
    }
}
