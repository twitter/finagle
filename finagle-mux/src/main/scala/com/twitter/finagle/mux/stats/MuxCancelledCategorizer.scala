package com.twitter.finagle.mux.stats

import com.twitter.finagle.mux.ClientDiscardedRequestException
import com.twitter.finagle.stats.CancelledCategorizer
import com.twitter.util.Throwables.RootCause

/**
 * Matcher for Throwables caused by a ClientDiscardedRequestException.
 */
object MuxCancelledCategorizer {
  def unapply(exc: Throwable): Option[ClientDiscardedRequestException] = {
    exc match {
      case t: ClientDiscardedRequestException => Some(t)
      case RootCause(MuxCancelledCategorizer(t)) => Some(t)
      case _ => None
    }
  }

  val Instance: PartialFunction[Throwable, String] = {
    case MuxCancelledCategorizer(_) => CancelledCategorizer.Cancelled
  }
}

