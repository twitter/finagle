package com.twitter.finagle.postgres.connection

import com.twitter.finagle.postgres.messages.{PgResponse, DataRow, Field}

import scala.collection.mutable.ListBuffer

/*
 * Connection states.
 */
trait State

case object AuthenticationRequired extends State

case object AuthenticationInProgress extends State

case object AwaitingPassword extends State

case class AggregatingAuthData(statuses: Map[String, String], processId: Int, secretKey: Int) extends State

case object Connected extends State

case object Parsing extends State

case object Binding extends State

case object SimpleQuery extends State

case object ExecutePreparedStatement extends State

case object Syncing extends State

case object AwaitParamsDescription extends State

case class AggregateRows(fields: IndexedSeq[Field], buff: ListBuffer[DataRow] = ListBuffer()) extends State

case class AggregateRowsWithoutFields(buff: ListBuffer[DataRow] = ListBuffer()) extends State

case class AwaitRowDescription(types: IndexedSeq[Int]) extends State

case class EmitOnReadyForQuery[R <: PgResponse](emit: R) extends State
