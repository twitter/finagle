package com.twitter.finagle.postgres.connection

import com.twitter.logging.Logger

object Undefined extends PartialFunction[Any, Nothing] {
  def isDefinedAt(o: Any) = false

  def apply(o: Any) = sys.error("undefined")
}

/*
 * Generic definition of a state machine. Used to define connection state machine
 * in Connection.scala.
 */
trait StateMachine[E, R, S] {
  type Transition = PartialFunction[(E, S), (Option[R], S)]

  private[this] val logger = Logger("state machine")
  private[this] var transitionFunction: Transition = Undefined

  @volatile private[this] var currentState: S = _

  def startState(s : S) {
    currentState = s
  }

  def transition(t: Transition) {
    transitionFunction = transitionFunction orElse t
  }

  private[this] def handleMisc: PartialFunction[(E, S), (Option[R], S)] = {
    case (e, s) => throw new IllegalArgumentException("Unknown event " + e + " for state " + s)
  }

  def onEvent(e: E): Option[R] = {
    val f = transitionFunction.orElse(handleMisc)
    logger.ifDebug("Received event in state %s".format(currentState.getClass.getName))

    val (result, newState) = f((e, currentState))
    logger.ifDebug("Transitioning to state %s and emiting result".format(newState.getClass.getName))

    currentState = newState
    result
  }
}
