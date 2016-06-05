package com.twitter.finagle.http.codec

import com.twitter.finagle._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.http.Message
import com.twitter.logging.Logger
import com.twitter.util.{NonFatal, Time}

object HttpContext {

  private[this] val Prefix = "Finagle-Ctx-"
  private[this] val DeadlineHeaderKey = Prefix+Deadline.id

  private val log = Logger(getClass.getName)

  /**
   * Remove the Deadline header from the Message. May be used
   * when it is not desirable for clients to be able to set
   * bogus or expired Deadline headers in an HTTP request.
   */
  def removeDeadline(msg: Message): Unit =
    msg.headerMap.remove(DeadlineHeaderKey)

  private[this] def marshalDeadline(deadline: Deadline): String =
    deadline.timestamp.inNanoseconds + " " + deadline.deadline.inNanoseconds

  private[this] def unmarshalDeadline(header: String): Option[Deadline] =
    try {
      val values = header.split(' ')
      val timestamp = values(0).toLong
      val deadline = values(1).toLong
      Some(Deadline(Time.fromNanoseconds(timestamp), Time.fromNanoseconds(deadline)))
    } catch {
      case NonFatal(exc) =>
        log.debug(s"Could not unmarshall Deadline from header value: ${header}")
        None
    }

  /**
   * Read Finagle-Ctx header pairs from the given message for Contexts:
   *     - Deadline
   * and run `fn`.
   */
  private[http] def read[R](msg: Message)(fn: => R): R =
    msg.headerMap.get(DeadlineHeaderKey) match {
      case Some(str) =>
        unmarshalDeadline(str) match {
          case Some(deadline) => Contexts.broadcast.let(Deadline, deadline)(fn)
          case None => fn
        }
      case None =>
        fn
    }

  /**
   * Write Finagle-Ctx header pairs into the given message for Contexts:
   *     - Deadline
   */
  private[http] def write(msg: Message): Unit =
    Deadline.current match {
      case Some(deadline) =>
        msg.headerMap.set(DeadlineHeaderKey, marshalDeadline(deadline))
      case None =>
    }
}
