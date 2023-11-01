package com.twitter.finagle.http.codec.context

import com.twitter.finagle.context.Contexts
import com.twitter.finagle.context.Requeues
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Try
import scala.util.control.NonFatal

private object HttpRequeues extends HttpContext {

  type ContextKeyType = Requeues
  val key: Contexts.broadcast.Key[Requeues] = Requeues

  def toHeader(requeues: Requeues): String = {
    requeues.attempt.toString
  }

  def fromHeader(header: String): Try[Requeues] = {
    try {
      Return(Requeues(header.toInt))
    } catch {
      case NonFatal(_) => Throw(new NumberFormatException)
    }
  }
}
