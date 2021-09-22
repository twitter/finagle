package com.twitter.finagle.postgresql.machine

import com.twitter.finagle.postgresql.BackendMessage
import com.twitter.finagle.postgresql.FrontendMessage
import com.twitter.finagle.postgresql.PgSqlInvalidMachineStateError
import com.twitter.finagle.postgresql.Response
import com.twitter.finagle.postgresql.BackendMessage.ReadyForQuery
import com.twitter.finagle.transport.Transport
import com.twitter.util.Future
import com.twitter.util.Promise
import com.twitter.util.Return
import com.twitter.util.Throw

/**
 * The runner connects state machines to a connection and allows dispatching machines on the connection.
 *
 * @param connection the connection to dispatch machines onto.
 */
class Runner(connection: Transport[FrontendMessage, BackendMessage]) {

  private[this] def run[R <: Response](
    machine: StateMachine[R],
    serviceResponsePromise: Promise[R]
  ): Future[ReadyForQuery] = {

    // NOTE: this is initialized to null, but is immediately set to the start state before doing anything else.
    var state: machine.State = null.asInstanceOf[machine.State]

    def step(transition: StateMachine.TransitionResult[machine.State, R]): Future[ReadyForQuery] =
      transition match {
        case StateMachine.Transition(s, action) =>
          state = s
          val doAction = action match {
            case StateMachine.NoOp => Future.Done
            case StateMachine.Send(msg) => connection.write(msg)
            case StateMachine.SendSeveral(msgs @ _*) =>
              Future
                .traverseSequentially(msgs) { s =>
                  connection.write(s)
                }.unit
            case StateMachine.Respond(r) =>
              serviceResponsePromise.updateIfEmpty(r)
              Future.Done
          }
          doAction before readAndStep
        case StateMachine.Complete(ready, response) =>
          response.foreach(serviceResponsePromise.updateIfEmpty)

          // Make sure the state machine produced a response for the client.
          if (!serviceResponsePromise.isDefined) {
            // NOTE: we still use updateIfEmpty since the promise may still be canceled.
            serviceResponsePromise.updateIfEmpty(
              Throw(
                PgSqlInvalidMachineStateError(
                  s"State machine ${machine.getClass} was in state $state and completed without producing a response for the client."
                ))
            )
          }
          Future.value(ready)
      }

    def readAndStep: Future[ReadyForQuery] =
      connection.read().flatMap(msg => step(machine.receive(state, msg)))

    step(machine.start)
  }

  /**
   * Runs a state machine to completion and fulfills the client response.
   *
   * @param serviceResponsePromise A promise that represents the result of applying the service.  Once complete, the
   *                               machine may continue running however, eg for streaming responses.
   * @return a `Future` which is fulfilled when the connection is available to dispatch another machine.
   */
  def dispatch[R <: Response](
    machine: StateMachine[R],
    serviceResponsePromise: Promise[R]
  ): Future[Unit] =
    run(machine, serviceResponsePromise).respond {
      case Return(_) => ()
      case t: Throw[_] =>
        serviceResponsePromise.updateIfEmpty(t.cast)
        connection.close()
    }.unit
}
