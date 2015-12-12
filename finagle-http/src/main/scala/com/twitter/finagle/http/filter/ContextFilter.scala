package com.twitter.finagle.http.filter

import com.twitter.finagle._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.http.Request
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
