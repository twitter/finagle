package com.twitter.finagle.httpx.service

import com.twitter.finagle.Service
import com.twitter.finagle.httpx.{Status, Request, Response}
import com.twitter.util.Future


/**
 * NotFoundService just returns 404 Not Found.
 */
class NotFoundService[REQUEST <: Request] extends Service[REQUEST, Response] {
  def apply(request: REQUEST): Future[Response] = {
    val response = request.response
    response.status = Status.NotFound
    Future.value(response)
  }
}

object NotFoundService extends NotFoundService[Request]
