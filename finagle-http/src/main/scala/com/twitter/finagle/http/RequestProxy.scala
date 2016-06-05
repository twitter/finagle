package com.twitter.finagle.http

import com.twitter.io.{Writer, Reader}
import com.twitter.util.Closable
import org.jboss.netty.handler.codec.http.HttpRequest

/**
 * Proxy for Request.  This can be used to create a richer request class
 * that wraps Request without exposing the underlying netty http type.
 */
abstract class RequestProxy extends Request {
  def request: Request

  override def ctx = request.ctx

  protected[finagle] def httpRequest: HttpRequest = request.httpRequest


  override def reader: Reader = request.reader
  override def writer: Writer with Closable = request.writer

  override def params = request.params
  def remoteSocketAddress = request.remoteSocketAddress

  override lazy val response = request.response
}
