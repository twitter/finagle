package com.twitter.finagle.health

/**
 * specifies why a server is unhealthy. currently the only
 * health metric we have is open connections
 */
object UnhealthyReason extends Enumeration {
  type UnhealthyReason = Value
  val TooManyOpenConnections = Value
}

/**
 * a HealthEvent is used to indicate a state change between a healthy server
 * and an unhealthy one. If transitioning to Unhealthy, an UnhealthyReason
 * is provided
 */
sealed trait HealthEvent
case class Healthy() extends HealthEvent
case class Unhealthy(reason: UnhealthyReason.Value) extends HealthEvent

/**
 * A HealthEventCallback is added to the ServerBuilder to allow the server
 * to communicate HealthEvents to the calling code.
 *
 * Example of usage:
 *
 * val callback = {
 *   case Healthy() => // handle healthy state change
 *   case Unhealthy(UnhealthyReason.TooManyOpenConnections) => // handle specific reason
 *   case Unhealthy(_) => // handle other reasons
 * }
 */

object NullHealthEventCallback extends (HealthEvent => Unit) {
  override def apply(e: HealthEvent) {}
}