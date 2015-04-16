package com.twitter.finagle.http

import com.twitter.io.{Writer, Reader}
import com.twitter.util.Closable

/**
 * Proxy for Request.  This can be used to create a richer request class
 * that wraps Request.
 */
abstract class RequestProxy extends Request {
  def request: Request
  def getRequest(): Request = request

  override def ctx = request.ctx

  override def httpRequest = request
  override def httpMessage = request

  override def reader: Reader = request.reader
  override def writer: Writer with Closable = request.writer

  override def params = request.params
  def remoteSocketAddress = request.remoteSocketAddress

  override lazy val response = request.response
}
