package com.twitter.finagle.http

import org.jboss.netty.handler.codec.http.HttpResponse

abstract class ResponseProxy extends Response {
  def response: Response
  override def isRequest                 = response.isRequest
  override def status: Status            = response.status
  override def status_=(value: Status)   = response.status_=(value)
  override def statusCode: Int           = response.statusCode
  override def statusCode_=(value: Int)  = response.statusCode_=(value)
  override def getStatusCode(): Int      = response.getStatusCode()
  override def setStatusCode(value: Int) = response.setStatusCode(value)
  override def encodeString(): String    = response.encodeString()

  protected[finagle] def httpResponse: HttpResponse = response.httpResponse
}
