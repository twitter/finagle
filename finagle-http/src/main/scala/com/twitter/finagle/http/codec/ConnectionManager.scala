package com.twitter.finagle.http.codec

import com.twitter.finagle.http.{Message, Request, Response}
import com.twitter.util.Future

/**
 * The HTTP connection manager implements connection management in
 * accordance with RFC 2616 § 8. This is just the state machine; the
 * codec implementations are in {Server,Client}ConnectionManager.
 */
private[finagle] class ConnectionManager {
  private[this] var isKeepAlive = false
  private[this] var isIdle = true
  private[this] var activeStreams = 0

  def observeMessage(message: Message, onFinish: Future[Unit]): Unit = synchronized {
    message match {
      case req: Request => observeRequest(req, onFinish)
      case rep: Response => observeResponse(rep, onFinish)
      case _ => isKeepAlive = false  // conservative
    }
  }

  def observeRequest(request: Request, onFinish: Future[Unit]): Unit = synchronized {
    isIdle = false
    isKeepAlive = request.isKeepAlive
    handleIfStream(onFinish)
  }

  def observeResponse(response: Response, onFinish: Future[Unit]): Unit = synchronized {
    if ((!response.isChunked && response.contentLength.isEmpty) || !response.isKeepAlive) isKeepAlive = false

    // If a response isn't chunked, then we're done with this request,
    // and hence idle.
    isIdle = !response.isChunked
    handleIfStream(onFinish)
  }

  // this can be unsynchronized because all callers are synchronized.
  private[this] def handleIfStream(onFinish: Future[Unit]): Unit = {
    if (!onFinish.isDefined) {
      activeStreams += 1
      onFinish.ensure {
        endStream()
      }
    }
  }

  private[this] def endStream(): Unit = synchronized {
    activeStreams -= 1
    isIdle = activeStreams == 0
  }

  def shouldClose(): Boolean = synchronized { isIdle && !isKeepAlive }
}
