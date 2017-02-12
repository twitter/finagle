package com.twitter.finagle.http.codec

import com.twitter.finagle.context.{Deadline, Contexts, Retries}
import com.twitter.finagle.http.Message
import com.twitter.logging.{Level, Logger}
import com.twitter.util.Time
import scala.util.control.NonFatal

object HttpContext {

  private[this] val Prefix = "Finagle-Ctx-"
  private[this] val DeadlineHeaderKey = Prefix+Deadline.id
  private[this] val RetriesHeaderKey = Prefix+Retries.id

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

  private[this] def marshalRetries(retries: Retries): String =
    retries.retries.toString


  private[this] val unmarshalDeadline: String => Option[Deadline] = header => {
    try {
      val values = header.split(' ')
      val timestamp = values(0).toLong
      val deadline = values(1).toLong
      Some(Deadline(Time.fromNanoseconds(timestamp), Time.fromNanoseconds(deadline)))
    } catch {
      case NonFatal(exc) =>
        if (log.isLoggable(Level.DEBUG))
          log.debug(s"Could not unmarshal Deadline from header value: ${header}")
        None
    }
  }

  private[this] val unmarshalRetries: String => Option[Retries] = header => {
    try {
      Some(Retries(header.toInt))
    } catch {
      case NonFatal(exc) =>
        if (log.isLoggable(Level.DEBUG))
          log.debug(s"Could not unmarshal Retries from header value: ${header}")
        None
    }
  }

  /**
   * Read Finagle-Ctx header pairs from the given message for Contexts:
   *     - Deadline
   *     - Retries
   * and run `fn`.
   */
  private[http] def read[R](msg: Message)(fn: => R): R = {
    var ctxValues: List[Contexts.broadcast.KeyValuePair[_]] = Nil

    msg.headerMap.get(DeadlineHeaderKey).flatMap(unmarshalDeadline) match {
      case Some(deadline) => ctxValues = Contexts.broadcast.KeyValuePair(Deadline, deadline)::ctxValues
      case None =>
    }

    msg.headerMap.get(RetriesHeaderKey).flatMap(unmarshalRetries) match {
      case Some(retries) => ctxValues = Contexts.broadcast.KeyValuePair(Retries, retries)::ctxValues
      case None =>
    }

    Contexts.broadcast.let(ctxValues)(fn)
  }

  /**
   * Write Finagle-Ctx header pairs into the given message for Contexts:
   *     - Deadline
   *     - Retries
   */
  private[http] def write(msg: Message): Unit = {
    Deadline.current match {
      case Some(deadline) =>
        msg.headerMap.set(DeadlineHeaderKey, marshalDeadline(deadline))
      case None =>
    }

    Retries.current match {
      case Some(retries) =>
        msg.headerMap.set(RetriesHeaderKey, marshalRetries(retries))
      case None =>
    }
  }
}
