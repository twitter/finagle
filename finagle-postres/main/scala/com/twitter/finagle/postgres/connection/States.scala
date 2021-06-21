package com.twitter.finagle.postgres.connection

import com.twitter.concurrent.AsyncStream
import com.twitter.finagle.postgres.messages.{DataRow, PgResponse}
import com.twitter.util.Promise

import scala.collection.mutable.ListBuffer

/*
 * Connection states.
 */
sealed trait State



case object SimpleQuery extends State

case object RequestingSsl extends State

case object AwaitingSslResponse extends State

case object AuthenticationRequired extends State

case object AuthenticationInProgress extends State

case object AwaitingPassword extends State

case class AggregatingAuthData(statuses: Map[String, String], processId: Int, secretKey: Int) extends State

case object Connected extends State

case object Syncing extends State

case object Terminated extends State

// All of the extended query states - Sync can be issued while in these states
sealed trait ExtendedQueryState extends State

case object Parsing extends ExtendedQueryState

case object Binding extends ExtendedQueryState

case object ExecutePreparedStatement extends ExtendedQueryState

case object AwaitParamsDescription extends ExtendedQueryState

case class StreamRows(complete: Promise[Unit], extended: Boolean, nextRow: Promise[AsyncStream[DataRow]] = new Promise) extends ExtendedQueryState {
  val asyncStream: AsyncStream[DataRow] = AsyncStream.fromFuture(nextRow).flatten
}

case class AwaitRowDescription(types: Array[Int]) extends ExtendedQueryState

case class EmitOnReadyForQuery[R <: PgResponse](emit: StateMachine.TransitionResult[R]) extends ExtendedQueryState
object EmitOnReadyForQuery {
  def apply[R <: PgResponse](response: R): EmitOnReadyForQuery[R] =
    EmitOnReadyForQuery(StateMachine.Response(response))
}
