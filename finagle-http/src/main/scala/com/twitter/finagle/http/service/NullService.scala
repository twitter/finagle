package com.twitter.finagle.http.service

import com.twitter.finagle.Service
import com.twitter.finagle.http.{Request, Response}
import com.twitter.util.Future

/*** A null Service.  Useful for testing. */
class NullService[REQUEST <: Request] extends Service[REQUEST, Response] {
  def apply(request: REQUEST): Future[Response] =
    Future.value(request.response)
}

object NullService extends NullService[Request]
