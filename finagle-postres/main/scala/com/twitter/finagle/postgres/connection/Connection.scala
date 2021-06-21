package com.twitter.finagle.postgres.connection

import java.util.concurrent.atomic.AtomicInteger

import com.twitter.finagle.postgres.messages._
import com.twitter.logging.Logger
import scala.collection.mutable.ListBuffer

/*
 * Representation of a single Postgres connection.
 */
class Connection(startState: State = AuthenticationRequired) {
  val id = Connection.nextId()
  private[this] val logger = Logger(s"${getClass.getName}.connection-$id")
  private[this] val stateMachine = new ConnectionStateMachine(startState, id)


  def send(msg: FrontendMessage) = {
    logger.ifDebug("Sent frontend message of type: %s".format(msg.getClass.getName))

    msg match {
      case q: Query =>
        logger.ifDebug("Query: %s".format(q.str))
      case _ =>
    }

    stateMachine.onEvent(msg)
  }

  def receive(msg: BackendMessage): Option[PgResponse] = {
    logger.ifDebug("Received backend message of type: %s".format(msg.getClass.getName))

    val result = stateMachine.onEvent(msg)
    result foreach {
      r => logger.ifDebug(s"Emitting result ${r.getClass.getName}")
    }
    result
  }
}

object Connection {
  private[this] val currentId = new AtomicInteger(0)
  private def nextId() = currentId.getAndIncrement()
}