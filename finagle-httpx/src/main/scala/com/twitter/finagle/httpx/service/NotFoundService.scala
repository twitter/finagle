package com.twitter.finagle.httpx.service

import com.twitter.finagle.Service
import com.twitter.finagle.httpx.{Status, Ask, Response}
import com.twitter.util.Future


/**
 * NotFoundService just returns 404 Not Found.
 */
class NotFoundService[ASK <: Ask] extends Service[ASK, Response] {
  def apply(request: ASK): Future[Response] = {
    val response = request.response
    response.status = Status.NotFound
    Future.value(response)
  }
}

object NotFoundService extends NotFoundService[Ask]
