package com.twitter.finagle.postgres.protocol

import com.twitter.finagle.postgres.connection.{ConnectionStateMachine, State}
import com.twitter.finagle.postgres.messages.{PgResponse, FrontendMessage, BackendMessage}
import org.specs2.specification.Example
import org.specs2.mutable.{ArgumentsArgs, FragmentsBuilder}

import scala.language.implicitConversions

trait ConnectionSpec {
  this: FragmentsBuilder with ArgumentsArgs =>

  args(sequential = true)

  implicit var connection = new ConnectionStateMachine()

  def withConnection(block: => Unit) {
    connection = new ConnectionStateMachine()
    block
  }

  implicit def inConnectionExample(s: String): InConnection = new InConnection(new InExample(s))

  class InConnection(underlying: InExample) {
    def inConnection(block: => Unit): Example = underlying.in(withConnection(block))
  }


  private[this] var result: Option[PgResponse] = None

  def send(msg: FrontendMessage)(implicit connection: ConnectionStateMachine) = {
    connection.onEvent(msg)
    result = None
  }

  def receive(msg: BackendMessage)(implicit connection: ConnectionStateMachine) = {
    result = connection.onEvent(msg)
  }

  def setState(state: State) {
    connection = new ConnectionStateMachine(state)
  }

  def response = result

}
