package com.twitter.finagle.mux

import com.twitter.finagle.FailureFlags
import com.twitter.logging.HasLogLevel
import scala.util.control.NoStackTrace

/**
 * Indicates that a client requested that a given request be discarded.
 *
 * This implies that the client issued a Tdiscarded message for a given tagged
 * request, as per [[com.twitter.finagle.mux]].
 */
class ClientDiscardedRequestException private[mux] (why: String, val flags: Long)
    extends Exception(why)
    with FailureFlags[ClientDiscardedRequestException]
    with HasLogLevel
    with NoStackTrace {
  def logLevel: com.twitter.logging.Level = com.twitter.logging.Level.DEBUG

  def this(why: String) = this(why, FailureFlags.Interrupted)

  def copyWithFlags(newFlags: Long): ClientDiscardedRequestException =
    new ClientDiscardedRequestException(why, newFlags)
}

/**
 * Indicates that the server failed to interpret or act on the request. This
 * could mean that the client sent a [[com.twitter.finagle.mux]] message type
 * that the server is unable to process.
 */
case class ServerError(what: String) extends Exception(what) with NoStackTrace

/**
 * Indicates that the server encountered an error whilst processing the client's
 * request. In contrast to [[com.twitter.finagle.mux.ServerError]], a
 * ServerApplicationError relates to server application failure rather than
 * failure to interpret the request.
 */
case class ServerApplicationError(what: String) extends Exception(what) with NoStackTrace
