package com.twitter.finagle.http.codec.context

import com.twitter.finagle.context.{Contexts, Retries}
import com.twitter.util.{Return, Throw, Try}
import scala.util.control.NonFatal

private object HttpRetries extends HttpContext {

  type ContextKeyType = Retries
  val key: Contexts.broadcast.Key[Retries] = Retries

  def toHeader(retries: Retries): String = {
    retries.attempt.toString
  }

  def fromHeader(header: String): Try[Retries] = {
    try {
      Return(Retries(header.toInt))
    } catch {
      case NonFatal(e) => Throw(new NumberFormatException)
    }
  }
}
