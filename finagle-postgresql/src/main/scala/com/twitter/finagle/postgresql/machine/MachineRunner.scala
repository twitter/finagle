package com.twitter.finagle.postgresql.machine

import com.twitter.finagle.postgresql.Messages
import com.twitter.finagle.postgresql.transport.Packet
import com.twitter.finagle.transport.Transport
import com.twitter.util.Future

case class MachineRunner[S, R](transport: Transport[Packet, Packet], machine: StateMachine[S, R]) {

  var state: S = _

  private[this] def readMsg = transport.read().map(Messages.BackendMessage.read)

  private[this] def step(transition: StateMachine.TransitionResult[S, R]): Future[R] = transition match {
    case StateMachine.Transition(s, action) =>
      state = s
      action.fold(Future.Done) { msg => transport.write(msg.write) } before readAndStep
    case StateMachine.Complete(result) => Future.value(result)
  }
  private[this] def readAndStep =
    readMsg.flatMap { msg => step(machine.receive(state, msg)) }

  def run: Future[R] =
    step(machine.start)
}
