package com.twitter.finagle.httpx.filter

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.httpx.{Response, Ask}
import com.twitter.util.Future
import scala.collection.Map

class AddResponseHeadersFilter(responseHeaders: Map[String, String])
    extends SimpleFilter[Ask, Response] {
  def apply(request: Ask, service: Service[Ask, Response]): Future[Response] = {
    service(request) map { response =>
      response.headerMap ++= responseHeaders
      response
    }
  }
}
