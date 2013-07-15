package com.twitter.finagle.service

import com.twitter.finagle.{NotServableException, SimpleFilter, Service}
import com.twitter.util.Future

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
