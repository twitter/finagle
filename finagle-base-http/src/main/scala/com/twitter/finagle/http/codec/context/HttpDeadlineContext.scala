package com.twitter.finagle.http.codec.context

import com.twitter.finagle.context.Deadline
import com.twitter.util.{Return, Throw, Time, Try}
import scala.util.control.NonFatal

private object HttpDeadline extends HttpContext {

  type ContextKeyType = Deadline
  val key = Deadline

  def toHeader(deadline: Deadline): String = {
    deadline.timestamp.inNanoseconds + " " + deadline.deadline.inNanoseconds
  }

  def fromHeader(header: String): Try[Deadline] = {
    try {
      val values = header.split(' ')
      val timestamp = values(0).toLong
      val deadline = values(1).toLong

      Return(
        Deadline(Time.fromNanoseconds(timestamp), Time.fromNanoseconds(deadline))
      )
    } catch {
      case NonFatal(e) => Throw(e)
    }
  }
}
