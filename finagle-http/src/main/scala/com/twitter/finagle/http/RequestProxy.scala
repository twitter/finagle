package com.twitter.finagle.http

/**
 * Proxy for Request.  This can be used to create a richer request class
 * that wraps Request.
 */
abstract class RequestProxy extends Request {
  def request: Request
  def getRequest(): Request = request

  override def httpRequest = request
  override def httpMessage = request

  override lazy val params = request.params
  def remoteSocketAddress = request.remoteSocketAddress

  override lazy val response = request.response
}
