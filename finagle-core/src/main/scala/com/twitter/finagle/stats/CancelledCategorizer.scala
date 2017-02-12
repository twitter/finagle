package com.twitter.finagle.stats

import com.twitter.finagle.{CancelledConnectionException, CancelledRequestException}
import com.twitter.util.Throwables.RootCause

/**
 * Matcher for Throwables caused by some form of cancellation.
 */
object CancelledCategorizer {
  val Cancelled = "cancelled"

  def unapply(exc: Throwable): Option[Throwable] = {
    exc match {
      case t: CancelledRequestException => Some(t)
      case t: CancelledConnectionException => Some(t)
      case RootCause(CancelledCategorizer(t)) => Some(t)
      case _ => None
    }
  }

  val Instance: PartialFunction[Throwable, String] = {
    case CancelledCategorizer(_) => Cancelled
  }
}
