package com.twitter.finagle.postgres.connection

import com.twitter.logging.Logger
import com.twitter.util.{Promise, Try}

object Undefined extends PartialFunction[Any, Nothing] {
  def isDefinedAt(o: Any) = false

  def apply(o: Any) = sys.error("undefined")
}

case class WrongStateForEvent[E, S](event: E, state: S)
  extends IllegalStateException("Unknown event " + event + " for state " + state)

/*
 * Generic definition of a state machine. Used to define connection state machine
 * in Connection.scala.
 */
trait StateMachine[E, R, S] {
  import StateMachine._
  type Transition = PartialFunction[(E, S), (Option[TransitionResult[R]], S)]
  type SimpleTransition = PartialFunction[(E, S), (Option[R], S)]

  val id: Int

  private[this] val logger = Logger(s"${getClass.getName}.state machine-$id")
  private[this] var transitionFunction: Transition = Undefined

  @volatile private[this] var currentState: S = _

  def startState(s : S) {
    currentState = s
  }

  def transition(t: SimpleTransition) {
    fullTransition(
      t.andThen {
        case (r, s) => (r.map(Response(_)), s)
      }
    )
  }

  def fullTransition(t: Transition) {
    transitionFunction = transitionFunction orElse t
  }

  private[this] def handleMisc: PartialFunction[(E, S), (Option[TransitionResult[R]], S)] = {
    case (e, s) => throw WrongStateForEvent(e, s)
  }

  def onEvent(e: E): Option[R] = {
    val f = transitionFunction.orElse(handleMisc)
    logger.ifDebug("Received event %s in state %s".format(e.getClass.getName, currentState.getClass.getName))

    val (result, newState) = f((e, currentState))
    logger.ifDebug("Transitioning to state %s and emiting result".format(newState.getClass.getName))

    currentState = newState

    result.flatMap {
      case Response(r) => Some(r)
      case Complete(promise, value) =>
        promise.update(value)
        None
    }
  }
}

object StateMachine {
  // The result of a transition
  //   This is either a response value or a Promise to fulfill (either successfully or not)
  //   In both cases, state machine's current state is updated before this result is returned / applied.
  sealed trait TransitionResult[+R]
  case class Response[R](value: R) extends TransitionResult[R]
  case class Complete(signal: Promise[Unit], value: Try[Unit]) extends TransitionResult[Nothing]
}
