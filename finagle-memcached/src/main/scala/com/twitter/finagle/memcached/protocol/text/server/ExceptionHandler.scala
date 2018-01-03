package com.twitter.finagle.memcached.protocol.text.server

import com.twitter.finagle.memcached.protocol.{ClientError, NonexistentCommand, ServerError}

object ExceptionHandler {
  private val ERROR = "ERROR".getBytes
  private val CLIENT_ERROR = "CLIENT_ERROR".getBytes
  private val SERVER_ERROR = "SERVER_ERROR".getBytes
  private val Newlines = "[\\r\\n]".r

  def format(e: Throwable): Seq[Array[Byte]] = e match {
    case e: NonexistentCommand =>
      Seq(ERROR)
    case e: ClientError =>
      Seq(CLIENT_ERROR, Newlines.replaceAllIn(e.getMessage, " ").getBytes)
    case e: ServerError =>
      Seq(SERVER_ERROR, Newlines.replaceAllIn(e.getMessage, " ").getBytes)
    case t =>
      throw t
  }
}
