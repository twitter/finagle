package com.twitter.finagle

import com.twitter.io.Buf
import com.twitter.util.{Duration, Time, Try}

@deprecated("Use `com.twitter.finagle.context.Deadline` instead", "2016-08-22")
class Deadline(timestamp: Time, deadline: Time)
  extends com.twitter.finagle.context.Deadline(timestamp, deadline)

@deprecated("Use `com.twitter.finagle.context.Deadline` instead", "2016-08-22")
object Deadline {

  def current: Option[context.Deadline] = context.Deadline.current

  def ofTimeout(timeout: Duration): context.Deadline = context.Deadline.ofTimeout(timeout)

  def combined(d1: Deadline, d2: Deadline): context.Deadline = context.Deadline.combined(d1, d2)

  def marshal(deadline: Deadline): Buf = context.Deadline.marshal(deadline)

  def tryUnmarshal(body: Buf): Try[context.Deadline] = context.Deadline.tryUnmarshal(body)
}
