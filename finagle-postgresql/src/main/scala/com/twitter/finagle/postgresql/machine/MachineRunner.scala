package com.twitter.finagle.postgresql.machine

import com.twitter.finagle.postgresql.Messages
import com.twitter.finagle.postgresql.transport.Packet
import com.twitter.finagle.transport.Transport
import com.twitter.util.Future

case class MachineRunner[S, R](transport: Transport[Packet, Packet], machine: StateMachine[S, R]) {

  var state: S = machine.init

  private[this] def readMsg = transport.read().map(Messages.BackendMessage.read)

  private[this] def step(transition: StateMachine.TransitionResult[S, R]): Future[R] = transition match {
    case StateMachine.Send(s, msg) =>
      state = s
      transport.write(msg.write) before readAndStep
    case StateMachine.Complete(result) => Future.value(result)
    case StateMachine.Noop(s) =>
      state = s
      readAndStep
  }
  private[this] def readAndStep =
    readMsg.flatMap { msg => step(machine.receive(state, msg)) }

  def run: Future[R] =
    step(machine.start)
}
