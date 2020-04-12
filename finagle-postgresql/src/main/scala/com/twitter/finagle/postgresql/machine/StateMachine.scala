package com.twitter.finagle.postgresql.machine

import com.twitter.finagle.postgresql.Messages.BackendMessage
import com.twitter.finagle.postgresql.Messages.FrontendMessage

trait StateMachine[S, R] {
  def init: S
  def start: StateMachine.TransitionResult[S, R]
  def receive(state: S, msg: BackendMessage): StateMachine.TransitionResult[S, R]
  def send(state: S, msg: FrontendMessage): StateMachine.TransitionResult[S, R]
}
object StateMachine {
  sealed trait TransitionResult[+S, +R]
  case class Noop[S](state: S) extends TransitionResult[S, Nothing]
  case class Send[S](state: S, msg: FrontendMessage) extends TransitionResult[S, Nothing]
  case class Complete[R](value: R) extends TransitionResult[Nothing, R] // TODO: how do we move to the next machine?
}
