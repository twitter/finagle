package com.twitter.finagle.http

/**
 * Proxy for Ask.  This can be used to create a richer request class
 * that wraps Ask.
 */
abstract class AskProxy extends Ask {
  def request: Ask
  def getAsk(): Ask = request

  override def httpAsk = request
  override def httpMessage = request

  override def params = request.params
  def remoteSocketAddress = request.remoteSocketAddress

  override lazy val response = request.response
}
