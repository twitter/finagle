package com.twitter.finagle.http.filter

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.http.{Response, Request}
import com.twitter.util.Future
import scala.collection.Map

class AddResponseHeadersFilter(responseHeaders: Map[String, String])
    extends SimpleFilter[Request, Response] {
  def apply(request: Request, service: Service[Request, Response]): Future[Response] = {
    service(request) map { response =>
      response.headerMap ++= responseHeaders
      response
    }
  }
}
