package com.twitter.finagle.service

import com.twitter.finagle.{NotServableException, SimpleFilter, Service}
import com.twitter.util.Future

/**
 * A [[com.twitter.finagle.Filter]] that uses an argument function to predicate
 * whether or not to apply the subsequent [[com.twitter.finagle.Service]]. In
 * cases where the function returns false, a the filter fails with a
 * [[com.twitter.finagle.NotServableException]].
 */
class OptionallyServableFilter[Req, Rep](f: Req => Future[Boolean])
  extends SimpleFilter[Req, Rep]
{
  private[this] val notServableException = new NotServableException

  def apply(req: Req, service: Service[Req, Rep]) = {
    f(req) flatMap {
      case true => service(req)
      case false => Future.exception(notServableException)
    }
  }
}
