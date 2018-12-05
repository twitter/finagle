package com.twitter.finagle.http

import com.twitter.finagle.{ChannelException, FailureFlags}
import java.net.SocketAddress

/** The Message was too long to be handled correctly */
final class TooLongMessageException private (
  ex: Option[Exception],
  remote: Option[SocketAddress],
  val flags: Long)
    extends ChannelException(ex, remote)
    with FailureFlags[TooLongMessageException] {

  protected def copyWithFlags(flags: Long): TooLongMessageException =
    new TooLongMessageException(ex, remote, flags)
}

object TooLongMessageException {
  def apply(ex: Exception, remote: SocketAddress): TooLongMessageException =
    new TooLongMessageException(Option(ex), Option(remote), FailureFlags.NonRetryable)

  def apply(ex: Exception): TooLongMessageException = apply(ex, null)
}
