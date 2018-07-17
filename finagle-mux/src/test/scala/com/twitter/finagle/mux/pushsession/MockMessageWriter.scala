package com.twitter.finagle.mux.pushsession

import com.twitter.finagle.mux.pushsession.MessageWriter.DiscardResult
import com.twitter.finagle.mux.transport.Message
import com.twitter.util.Future
import scala.collection.mutable

/** Helper class for testing what gets written into a real `MessageWriter` */
private class MockMessageWriter extends MessageWriter {
  val messages = new mutable.Queue[Message]()

  def write(message: Message): Unit = {
    messages += message
  }

  def removeForTag(id: Int): DiscardResult = DiscardResult.NotFound
  def drain: Future[Unit] = Future.Unit
}
