package com.twitter.finagle.http.codec

/**
 * The HTTP connection manager implements connection management in
 * accordance with RFC 2616 § 8. This is just the state machine: the
 * codec implementations are in {Server,Client}ConnectionManager.
 */

import org.jboss.netty.handler.codec.http._

class ConnectionManager {
  private[this] var isKeepAlive = false
  private[this] var isIdle = true

  def observeMessage(message: AnyRef) = synchronized {
    message match {
      case request: HttpRequest   => observeRequest(request)
      case response: HttpResponse => observeResponse(response)
      case chunk: HttpChunk       => observeChunk(chunk)
      case _                      => isKeepAlive = false  // conservative
    }
  }

  def observeRequest(request: HttpRequest) = synchronized {
    isIdle = false
    isKeepAlive = HttpHeaders.isKeepAlive(request)
  }

  def observeResponse(response: HttpResponse) = synchronized {
    if (!response.isChunked && !response.containsHeader(HttpHeaders.Names.CONTENT_LENGTH))
      isKeepAlive = false
    else if (!HttpHeaders.isKeepAlive(response))
      isKeepAlive = false

    // If a response isn't chunked, then we're done with this request,
    // and hence idle.
    isIdle = !response.isChunked
  }

  def observeChunk(chunk: HttpChunk) = synchronized {
    require(!isIdle)
    isIdle = chunk.isLast
  }

  def shouldClose() = synchronized { isIdle && !isKeepAlive }
}
