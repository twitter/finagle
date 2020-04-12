package com.twitter.finagle.postgresql.machine

import com.twitter.finagle.postgresql.Messages
import com.twitter.finagle.postgresql.Messages.BackendMessage
import com.twitter.finagle.postgresql.Messages.FrontendMessage
import com.twitter.finagle.postgresql.transport.MessageEncoder
import com.twitter.util.Future

trait StateMachine[S, R] {
  def start: StateMachine.TransitionResult[S, R]
  def receive(state: S, msg: BackendMessage): StateMachine.TransitionResult[S, R]
}
object StateMachine {
  sealed trait TransitionResult[+S, +R]
  case class TransitionAndSend[S, M <: FrontendMessage](state: S, msg: M)(implicit val encoder: MessageEncoder[M]) extends TransitionResult[S, Nothing]
  case class Transition[S](state: S) extends TransitionResult[S, Nothing]
  case class Complete[R](value: R, signal: Future[Messages.ReadyForQuery]) extends TransitionResult[Nothing, R]

  /** A machine that sends a single frontend message and expects a ReadyForQuery response */
  def singleMachine[M <: Messages.FrontendMessage: MessageEncoder, R](msg: M)(f: Messages.ReadyForQuery => R): StateMachine[Unit, R] = new StateMachine[Unit, R] {
    override def start: TransitionResult[Unit, R] = TransitionAndSend((), msg)
    override def receive(state: Unit, msg: BackendMessage): TransitionResult[Unit, R] = msg match {
      case r: Messages.ReadyForQuery => Complete(f(r), Future.value(r))
      case msg => sys.error(s"unexpected response $msg")
    }
  }
}
