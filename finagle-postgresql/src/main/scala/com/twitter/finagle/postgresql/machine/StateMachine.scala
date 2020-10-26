package com.twitter.finagle.postgresql.machine

import com.twitter.finagle.postgresql.BackendMessage
import com.twitter.finagle.postgresql.BackendMessage.ReadyForQuery
import com.twitter.finagle.postgresql.FrontendMessage
import com.twitter.finagle.postgresql.PgSqlNoSuchTransition
import com.twitter.finagle.postgresql.PgSqlServerError
import com.twitter.finagle.postgresql.Response
import com.twitter.finagle.postgresql.transport.MessageEncoder
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Try

/**
 * A `StateMachine` is responsible for managing the connection state and expose a `Request => Future[Response]` regardless
 * of the number of actual request / response cycles required with a backend server.
 *
 * That is, for any particular client request / response cycle, multiple request / response cycles are usually required
 * with the postgresql backend. This requires coordinating state with the backend which is what is encapsulated in
 * `StateMachine` implementations.
 *
 * `StateMachine`s have a public type parameter to represent the response type that it will eventually produce.
 * They also have an internal type parameter to represent its internal state representation.
 *
 * `StateMachine` only encapsulates the state transitions, not the state itself, which must be maintained externally
 * (i.e.: in some kind of runner). That is, implementations provide a starting state and a way to transition to another
 * state. The "current state" must be maintained in whatever is encapsulating the connection
 * (specifically, in [[com.twitter.finagle.postgresql.ClientDispatcher]] for the finagle client.)
 *
 * `StateMachine`s are responsible for eventually producing the response to the client's request as well as eventually
 * reaching a completion state which leaves the connection ready for a new client request. Note that responding to the
 * client with the response does not necessarily occur at the same time the state machine completing. That is, the
 * client response may be produced while the state machine still exchanges messages with the backend on the connection.
 *
 * @see [[com.twitter.finagle.postgresql.ClientDispatcher]]
 * @tparam R The type of client response eventually produced by the state machine implementation.
 */
trait StateMachine[+R <: Response] {
  type State

  /**
   * The initial transition to execute upon starting this particular machine.
   * Normally, this will contain `Send` actions to send messages to the Postgres backend.
   *
   * @return the initial transition to execute upon starting the machine.
   */
  def start: StateMachine.TransitionResult[State, R]

  /**
   * Given the current state and a message from the backend server, this method will produce the appropriate transition
   * to execute.
   *
   * Specifically, this may be additional messages to send to the backend, a response to send to the client, etc.
   *
   * @param state the current state of the connection previously returned by a call to `start` or `receive`.
   * @param msg the message received from the backend server
   * @return the transition to execute to realize the new state
   */
  def receive(state: State, msg: BackendMessage): StateMachine.TransitionResult[State, R]
}
object StateMachine {

  /**
   * Represents all the possible side effects that can occur when transitioning to a new state.
   */
  sealed trait Action[+R <: Response]
  case object NoOp extends Action[Nothing]

  /**
   * Send the specified [[FrontendMessage]] after transitioning to the new sate.
   */
  case class Send[M <: FrontendMessage](msg: M)(implicit val encoder: MessageEncoder[M]) extends Action[Nothing]

  /**
   * Send multiple messages to the backend after transitioning to the new sate.
   * This allows sending multiple messages in a row without waiting for a message from the backend between each of them.
   */
  case class SendSeveral(msgs: Seq[Send[_ <: FrontendMessage]]) extends Action[Nothing]
  object SendSeveral {
    def apply[
      A <: FrontendMessage: MessageEncoder,
      B <: FrontendMessage: MessageEncoder
    ](a: A, b: B): SendSeveral = SendSeveral(Send(a) :: Send(b) :: Nil)

    def apply[
      A <: FrontendMessage: MessageEncoder,
      B <: FrontendMessage: MessageEncoder,
      C <: FrontendMessage: MessageEncoder,
    ](a: A, b: B, c: C): SendSeveral = SendSeveral(Send(a) :: Send(b) :: Send(c) :: Nil)

    def apply[
      A <: FrontendMessage: MessageEncoder,
      B <: FrontendMessage: MessageEncoder,
      C <: FrontendMessage: MessageEncoder,
      D <: FrontendMessage: MessageEncoder,
    ](a: A, b: B, c: C, d: D): SendSeveral = SendSeveral(Send(a) :: Send(b) :: Send(c) :: Send(d) :: Nil)
  }

  /**
   * Respond to the client's request. This effectively completes the client's request, but will
   * not release the connection yet.
   *
   * @param value the value used to fulfill the client's response promise.
   */
  case class Respond[R <: Response](value: Try[R]) extends Action[R]

  /**
   * The result of applying a message to a particular state.
   * This encapsulates the new state to transition to as well as possible side effects to produce.
   */
  sealed trait TransitionResult[+S, +R <: Response]
  case class Transition[S, R <: Response](state: S, action: Action[R]) extends TransitionResult[S, R]

  /**
   * Indicates that the state machine is finished and the connection may be released.
   * Postgres uses the [[ReadyForQuery]] message to indicate this to the client, so it is expected to be provided here.
   * Also, if completing the sate machine should also complete the client's response, then the value to produce
   * can be provided in the `response` field. If the client's promise has already been fulfilled, this will have
   * no effect.
   *
   * @param ready the [[ReadyForQuery]] message from the backend confirming the connection's state.
   * @param response an optional value to used to fulfill the client's promise. If this is [[None]], then the [[Respond]]
   *                 action must have been produced during the state machine's lifecycle to guarantee the client
   *                 receives a response.
   */
  case class Complete[R <: Response](ready: ReadyForQuery, response: Option[Try[R]])
      extends TransitionResult[Nothing, R]

  /**
   * A machine that sends a single frontend message and expects a ReadyForQuery response
   */
  def singleMachine[M <: FrontendMessage: MessageEncoder, R <: Response](
    name: String,
    msg: M
  )(f: BackendMessage.ReadyForQuery => R): StateMachine[R] = new StateMachine[R] {
    override type State = Unit
    override def start: TransitionResult[State, R] = Transition((), Send(msg))
    override def receive(state: State, msg: BackendMessage): TransitionResult[State, R] = msg match {
      case r: BackendMessage.ReadyForQuery => Complete(r, Some(Return(f(r))))
      case e: BackendMessage.ErrorResponse => Transition(Unit, Respond(Throw(PgSqlServerError(e))))
      case msg => throw PgSqlNoSuchTransition(name, (), msg)
    }
  }
}
