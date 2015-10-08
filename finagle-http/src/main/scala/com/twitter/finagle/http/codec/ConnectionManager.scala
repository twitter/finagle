package com.twitter.finagle.http.codec

import org.jboss.netty.handler.codec.http._

/**
 * The HTTP connection manager implements connection management in
 * accordance with RFC 2616 § 8. This is just the state machine: the
 * codec implementations are in {Server,Client}ConnectionManager.
 */
class ConnectionManager {
  private[this] var isKeepAlive = false
  private[this] var isIdle = true
  private[this] var chunks = 0

  def observeMessage(message: Any) = synchronized {
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
    if (request.isChunked) chunks += 1
  }

  def observeResponse(response: HttpResponse) = synchronized {
    if (!response.isChunked && !response.headers.contains(HttpHeaders.Names.CONTENT_LENGTH))
      isKeepAlive = false
    else if (!HttpHeaders.isKeepAlive(response))
      isKeepAlive = false

    // If a response isn't chunked, then we're done with this request,
    // and hence idle.
    isIdle = !response.isChunked

    if (response.isChunked) chunks += 1
  }

  def observeChunk(chunk: HttpChunk) = synchronized {
    require(!isIdle)
    if (chunk.isLast) chunks -= 1
    isIdle = chunks == 0
  }

  def shouldClose() = synchronized { isIdle && !isKeepAlive }
}
