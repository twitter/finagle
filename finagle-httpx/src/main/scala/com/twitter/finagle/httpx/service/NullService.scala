package com.twitter.finagle.httpx.service

import com.twitter.finagle.Service
import com.twitter.finagle.httpx.{Ask, Response}
import com.twitter.util.Future


/*** A null Service.  Useful for testing. */
class NullService[ASK <: Ask] extends Service[ASK, Response] {
  def apply(request: ASK): Future[Response] =
    Future.value(request.response)
}

object NullService extends NullService[Ask]
