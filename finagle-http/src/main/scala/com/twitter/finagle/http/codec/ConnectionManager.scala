package com.twitter.finagle.http.codec

import com.twitter.finagle.http.{Message, Request, Response}
import com.twitter.util.{Future, Promise}

/**
 * The HTTP connection manager implements connection management in
 * accordance with RFC 2616 § 8. This is just the state machine; the
 * codec implementations are in {Server,Client}ConnectionManager.
 */
private[finagle] class ConnectionManager {
  private[this] var isKeepAlive = false
  private[this] var isIdle = true
  private[this] var activeStreams, requestsObserved, responsesObserved = 0
  private[this] val closeP = new Promise[Unit]

  def observeMessage(message: Message, onFinish: Future[Unit]): Unit = synchronized {
    message match {
      case req: Request => observeRequest(req, onFinish)
      case rep: Response => observeResponse(rep, onFinish)
      case _ => isKeepAlive = false  // conservative
    }
  }

  def observeRequest(request: Request, onFinish: Future[Unit]): Unit = synchronized {
    requestsObserved += 1
    isIdle = false
    isKeepAlive = request.isKeepAlive
    handleIfStream(onFinish)
  }

  def observeResponse(response: Response, onFinish: Future[Unit]): Unit = synchronized {
    responsesObserved += 1
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
        if (shouldClose) closeP.setDone()
      }
    } else if (shouldClose) closeP.setDone()
  }

  private[this] def endStream(): Unit = synchronized {
    activeStreams -= 1
    isIdle = activeStreams == 0 && requestsObserved == responsesObserved
  }

  def shouldClose(): Boolean = synchronized { isIdle && !isKeepAlive }
  def onClose: Future[Unit] = closeP
}
