package com.twitter.finagle.http2.transport.client

import com.twitter.finagle.Status
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Closable, Future}

/**
 * Representation of an HTTP/2 multiplexing session.
 *
 * The `ClientSession` represents a single session that is the nexus
 * of serving multiple dispatches. It provides the ability to close
 * the shared session, obtain the shared session status, and generate
 * new child `Transport`s.
 */
private[http2] trait ClientSession extends Closable {

  /**
   * Create a new sub-transport backed by one or more streams
   * in an HTTP/2 session.
   *
   * @note Due to the nature of HTTP/2 which requires that a stream
   *       not be allocated until the HEADERS frame is written,
   *       successfully resolving a sub-transport does not mean a
   *       stream has been allocated and any dispatches may still
   *       fail.
   */
  def newChildTransport(): Future[Transport[Any, Any]]

  /**
   * Status representation of the health of a shared HTTP/2 session
   */
  def status: Status
}
