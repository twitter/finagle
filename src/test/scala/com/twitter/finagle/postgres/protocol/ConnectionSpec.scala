package com.twitter.finagle.postgres.protocol

import org.specs2.specification.Example
import org.specs2.mutable.{ArgumentsArgs, FragmentsBuilder}

trait ConnectionSpec {
  this: FragmentsBuilder with ArgumentsArgs =>

  args(sequential = true)

  implicit var connection: Connection = new Connection()

  def withConnection(block: => Unit) {
    connection = new Connection()
    block
  }

  implicit def inConnectionExample(s: String): InConnection = new InConnection(new InExample(s))

  class InConnection(underlying: InExample) {
    def inConnection(block: => Unit): Example = underlying.in(withConnection(block))
  }


  private[this] var result: Option[PgResponse] = None

  def send(msg: FrontendMessage)(implicit connection: Connection) = {
    connection.send(msg)
    result = None
  }

  def receive(msg: BackendMessage)(implicit connection: Connection) = {
    result = connection.receive(msg)
  }

  def response = result

}
