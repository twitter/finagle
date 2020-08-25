package com.twitter.finagle.exp.routing

import com.twitter.finagle.{Filter, Service}
import com.twitter.util.Future

/**
 * Filter to transform an input [[Req request]] to a [[UserReq user-facing request]] type.
 *
 * @param transform The function to transform from [[Req request]] to
 *                  [[UserReq user-facing request]].
 * @tparam Req The input request type.
 * @tparam Rep The output response type.
 * @tparam UserReq The user-facing request type of the [[Route route's]] underlying service.
 *
 * @note Any exceptions that occur as part of [[transform]] execution are *NOT* handled via this
 *       filter and propagated directly.
 */
private[routing] class RequestTransformingFilter[Req, Rep, UserReq](
  transform: Req => UserReq)
    extends Filter[Req, Rep, UserReq, Rep] {

  override final def apply(
    request: Req,
    service: Service[UserReq, Rep]
  ): Future[Rep] = {
    val routerRequest = transform(request)
    service(routerRequest)
  }
}
