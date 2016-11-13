package com.twitter.finagle.http.codec

import com.twitter.finagle.http.{Fields, Message, Request, Response}
import com.twitter.util.{Future, Promise}

/**
 * The HTTP connection manager implements connection management in
 * accordance with RFC 2616 § 8. This is just the state machine; the
 * codec implementations are in {Server,Client}ConnectionManager.
 */
private[finagle] class ConnectionManager {

  /** Indicates whether the connection should be closed when it becomes idle. */
  private[this] var isKeepAlive = false

  /** When false, the connection is busy servicing a request. */
  private[this] var isIdle = true

  /**
   * Indicates the number of chunked messages currently being
   * transmitted on this connection. Practically, on [0, 2].
   */
  private[this] var activeStreams = 0

  /**
   * Indicates the number of requests that have been issued that have
   * not yet received a response. Practically, on [0, 1].
   */
  private[this] var pendingResponses = 0

  /** Satisfied when the connection is ready to be torn down. */
  private[this] val closeP = new Promise[Unit]

  def observeMessage(message: Message, onFinish: Future[Unit]): Unit = synchronized {
    message match {
      case req: Request => observeRequest(req, onFinish)
      case rep: Response => observeResponse(rep, onFinish)
      case _ => isKeepAlive = false  // conservative
    }
  }

  def observeRequest(request: Request, onFinish: Future[Unit]): Unit = synchronized {
    pendingResponses += 1
    isIdle = false
    isKeepAlive = request.isKeepAlive
    handleIfStream(onFinish)
  }

  def observeResponse(response: Response, onFinish: Future[Unit]): Unit = synchronized {
    pendingResponses -= 1

    if (!isKeepAlive ||
        (!response.isChunked && response.contentLength.isEmpty) ||
        !response.isKeepAlive) {
      // We are going to close the connection after this response so we ensure that
      // the 'Connection' header is set to 'close' in order to give the client notice.
      response.headerMap.set(Fields.Connection, "close")
      isKeepAlive = false
    }

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
    isIdle = activeStreams == 0 && pendingResponses == 0
  }

  def shouldClose(): Boolean = synchronized { isIdle && !isKeepAlive }
  def onClose: Future[Unit] = closeP
}
