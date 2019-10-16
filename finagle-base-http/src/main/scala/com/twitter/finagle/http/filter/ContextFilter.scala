package com.twitter.finagle.http.filter

import com.twitter.finagle._
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.http.codec.context.HttpContext
import com.twitter.util.Future

/**
 * Sets the following Context values from the request headers:
 *     - request deadline
 */
private[finagle] class ServerContextFilter[Req <: Request, Rep] extends SimpleFilter[Req, Rep] {

  def apply(req: Req, service: Service[Req, Rep]): Future[Rep] =
    HttpContext.read(req)(service(req))
}

private[finagle] object ServerContextFilter {
  val role = Stack.Role("ServerContext")

  /**
   * A stack module that extracts context information and sets it on the local Context:
   *   - Dtab
   *   - Deadline
   */
  val module: Stackable[ServiceFactory[Request, Response]] =
    new Stack.Module0[ServiceFactory[Request, Response]] {
      val role = ServerContextFilter.role
      val description = "Extract context information from requests"

      val context = new ServerContextFilter[Request, Response]

      def make(next: ServiceFactory[Request, Response]) =
        context.andThen(next)
    }
}

object ServerDtabContextFilter {
  val role = Stack.Role("ServerDtabContext")

  /**
   * A stack module that extracts context information and sets it on the local Context:
   *   - Dtab
   */
  private[finagle] val module: Stackable[ServiceFactory[Request, Response]] =
    new Stack.Module0[ServiceFactory[Request, Response]] {
      val role = ServerDtabContextFilter.role
      val description = "Extract context information from requests"

      val dtab = new DtabFilter.Extractor

      def make(next: ServiceFactory[Request, Response]) =
        dtab.andThen(next)
    }
}

/**
 * Sets the following header values for the request Context:
 *     - request deadline
 */
private[finagle] class ClientContextFilter[Req <: Request, Rep] extends SimpleFilter[Req, Rep] {

  def apply(req: Req, service: Service[Req, Rep]): Future[Rep] = {
    HttpContext.write(req)
    service(req)
  }
}

private[finagle] object ClientContextFilter {
  val role = Stack.Role("ClientContext")

  /**
   * A stack module that sets Context information on outgoing requests:
   *   - Deadline
   */
  val module: Stackable[ServiceFactory[Request, Response]] =
    new Stack.Module0[ServiceFactory[Request, Response]] {
      val role = ClientContextFilter.role
      val description = "Set context information on outgoing requests"

      val context = new ClientContextFilter[Request, Response]

      def make(next: ServiceFactory[Request, Response]) =
        context.andThen(next)
    }
}

object ClientDtabContextFilter {
  val role = Stack.Role("DtabContext")

  /**
   * A stack module that sets Context information on outgoing requests:
   *   - Dtab
   */
  private[finagle] val module: Stackable[ServiceFactory[Request, Response]] =
    new Stack.Module0[ServiceFactory[Request, Response]] {
      val role = ClientDtabContextFilter.role
      val description = "Set dtab context information on outgoing requests"

      val dtab = new DtabFilter.Injector

      def make(next: ServiceFactory[Request, Response]) =
        dtab.andThen(next)
    }
}
