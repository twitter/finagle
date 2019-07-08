package com.twitter.finagle.http.codec

import com.twitter.finagle.http.{Fields, Message, Request, Response, Status}
import com.twitter.util.{Future, Promise}

/**
 * The HTTP connection manager implements connection management in
 * accordance with RFC 2616 § 8. This is just the state machine; the
 * codec implementations are in {Server,Client}ConnectionManager.
 */
private[finagle] class Http1ConnectionManager {

  /**
   * Indicates whether the connection should be closed when it becomes idle.
   * Because the connection is initially idle, we set this to `true` to avoid
   * the connection starting in a closed state.
   */
  private[this] var isKeepAlive = true

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
      case _ => isKeepAlive = false // conservative
    }
  }

  private[this] def observeRequest(request: Request, onFinish: Future[Unit]): Unit = synchronized {
    pendingResponses += 1
    isIdle = false
    isKeepAlive = request.keepAlive
    handleIfStream(onFinish)
  }

  private[this] def observeResponse(response: Response, onFinish: Future[Unit]): Unit =
    synchronized {
      pendingResponses -= 1

      if (!isKeepAlive || mustCloseOnFinish(response) || !response.keepAlive) {
        // We are going to close the connection after this response so we ensure that
        // the 'Connection' header is set to 'close' in order to give the client notice.
        response.headerMap.setUnsafe(Fields.Connection, "close")
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

  def shouldClose: Boolean = synchronized { isIdle && !isKeepAlive }
  def onClose: Future[Unit] = closeP

  private[this] def mustCloseOnFinish(resp: Response): Boolean = {
    // For a HTTP/1.x response that may have a body, the body length defined by
    // either the `Transfer-Encoding: chunked` mechanism, the Content-Length header,
    // or the end of the connection, in that order.
    // See https://tools.ietf.org/html/rfc7230#section-3.3.3 for more details.
    mayHaveContent(resp.status) && !resp.isChunked && !resp.headerMap.contains(Fields.ContentLength)
  }

  // Some status codes are not permitted to have a message body.
  private[this] def mayHaveContent(status: Status): Boolean = status match {
    case Status.Informational(_) => false // all 1xx status codes must not have a body
    case Status.NoContent => false // 204 No Content
    case Status.NotModified => false // 304 Not Modified
    case _ => true
  }
}
