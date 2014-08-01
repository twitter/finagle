package com.twitter.finagle.smtp

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.smtp.reply._
import com.twitter.finagle.transport.QueueTransport
import com.twitter.util.Await
import com.twitter.finagle.smtp.reply.ServiceReady
import com.twitter.finagle.smtp.reply.OK
import com.twitter.finagle.smtp.reply.InvalidReply

@RunWith(classOf[JUnitRunner])
class SmtpClientDispatcherTest extends FunSuite {
  def newTestSet = {
    val client = new AsyncQueue[Request]
    val server = new AsyncQueue[UnspecifiedReply]
    val transport = new QueueTransport[Request, UnspecifiedReply](client, server)
    (client, server, transport)
  }

  def newTestSetWithGreeting = {
    val (client, server, transport) = newTestSet
    server.offer(ServiceReady("testdomain","testgreet"))
    val dispatcher = new SmtpClientDispatcher(transport)
    (server, dispatcher)
  }

  test("receives correct greeting") {
    val (server, dispatcher) = newTestSetWithGreeting
    assert(dispatcher.isAvailable)
  }

  test("closes on incorrect greeting") {
    val (client, server, transport) = newTestSet
    server.offer(InvalidReply("wronggreet"))
    val dispatcher = new SmtpClientDispatcher(transport)
    assert(!dispatcher.isAvailable)
  }

  test("returns specified replies") {
    val (server, dispatcher) = newTestSetWithGreeting
    val specifiedOK = OK("specified")
    val rep = dispatcher(Request.Noop)
    server.offer(specifiedOK)
    assert(Await.result(rep) === specifiedOK)
  }

  test("specifies unspecified replies") {
    val (server, dispatcher) = newTestSetWithGreeting
    val unspecifiedOK = new UnspecifiedReply {
      val info: String = "unspecified"
      val code: Int = 250
    }
    val rep = dispatcher(Request.Noop)
    server.offer(unspecifiedOK)
    assert(Await.result(rep).isInstanceOf[OK])
  }

  test("errors are exceptions") {
    val (server, dispatcher) = newTestSetWithGreeting
    val rep = dispatcher(Request.Noop)
    server.offer(SyntaxError("error"))
    assert(rep.isThrow)
  }

  test("wraps unknown replies") {
    val (server, dispatcher) = newTestSetWithGreeting
    val unknownRep = new UnspecifiedReply {
      val info: String = "unknown"
      val code: Int = 666
    }
    val rep = dispatcher(Request.Noop)
    server.offer(unknownRep)
    assert(Await.result(rep).isInstanceOf[UnknownReplyCodeError])
  }
}
