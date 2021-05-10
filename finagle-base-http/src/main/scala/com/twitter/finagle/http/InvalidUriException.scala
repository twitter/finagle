package com.twitter.finagle.http

import com.twitter.finagle.{ChannelException, FailureFlags}
import com.twitter.logging.{HasLogLevel, Level}

private[finagle] object InvalidUriException {
  def apply(uri: CharSequence): InvalidUriException =
    new InvalidUriException(uri, FailureFlags.NonRetryable)
}

final class InvalidUriException private (
  uri: CharSequence,
  override val flags: Long)
    extends ChannelException(new IllegalArgumentException(s"Invalid URI: $uri"))
    with FailureFlags[InvalidUriException]
    with HasLogLevel {

  override protected def copyWithFlags(flags: Long): InvalidUriException =
    new InvalidUriException(uri, flags)

  override def logLevel: Level = Level.WARNING
}
