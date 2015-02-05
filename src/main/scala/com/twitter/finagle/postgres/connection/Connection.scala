package com.twitter.finagle.postgres.connection

import com.twitter.finagle.postgres.messages._
import com.twitter.logging.Logger

import scala.collection.mutable.ListBuffer

/*
 * Representation of a single Postgres connection.
 */
class Connection(startState: State = AuthenticationRequired) {
  private[this] val logger = Logger("connection")
  private[this] val stateMachine = new ConnectionStateMachine(startState)

  def send(msg: FrontendMessage) {
    logger.ifDebug("Sent frontend message of type: %s".format(msg.getClass.getName))

    msg match {
      case q: Query =>
        logger.ifDebug("Query: %s".format(q.str))
      case _ =>
    }

    val _ = stateMachine.onEvent(msg)
  }

  def receive(msg: BackendMessage): Option[PgResponse] = {
    logger.ifDebug("Received backend message of type: %s".format(msg.getClass.getName))

    val result = stateMachine.onEvent(msg)
    logger.ifDebug("Emitting result")
    result
  }
}
