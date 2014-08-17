package com.twitter.finagle.smtp

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.smtp.reply._
import com.twitter.finagle.smtp.reply.InvalidReply
import com.twitter.finagle.smtp.reply.OK
import com.twitter.finagle.smtp.reply.ServiceReady
import com.twitter.finagle.transport.QueueTransport
import com.twitter.util.Await

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
    dispatcher(Request.Hello)
    server.offer(ClosingTransmission("QUIT"))
    assert(!dispatcher.isAvailable)
  }

  test("aggregates multiline replies") {
    val (server, dispatcher) = newTestSetWithGreeting
    val frep = dispatcher(Request.Noop)

    server.offer(NonTerminalLine(250, "nonterminal"))
    assert(!frep.isDefined)

    server.offer(OK("terminal"))
    val rep = Await result frep

    assert(rep.isMultiline)
    assert(rep.code === 250)
    assert(rep.lines === Seq("nonterminal", "terminal"))
    assert(rep.info === "nonterminal")
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

  test("multiline replies with non-matching codes are incorrect") {
    val (server, dispatcher) = newTestSetWithGreeting
    val frep = dispatcher(Request.Noop)

    server.offer(NonTerminalLine(230, "nonterminal"))
    assert(!frep.isDefined)

    server.offer(OK("terminal"))

    frep onSuccess {
      _ => fail("should fail")
    } handle {
      case rep: InvalidReply =>
        assert(rep.isMultiline)
        assert(rep.code === 230)
      case _ => fail("should be InvalidReply")
    }
  }

  test("replies with InvalidReply are aggregated into InvalidReply") {
    val (server, dispatcher) = newTestSetWithGreeting
    val frep = dispatcher(Request.Noop)

    server.offer(NonTerminalLine(250, "nonterminal"))
    assert(!frep.isDefined)

    server.offer(InvalidReply("terminal"))

    frep onSuccess {
      _ => fail("should fail")
    } handle {
      case rep: InvalidReply =>
        assert(rep.isMultiline)
        assert(rep.code === 250)
      case _ => fail("should be InvalidReply")
    }
  }

  test("wraps unknown replies") {
    val (server, dispatcher) = newTestSetWithGreeting
    val unknownRep = new UnspecifiedReply {
      val info: String = "unknown"
      val code: Int = 666
    }
    val rep = dispatcher(Request.Noop)
    server.offer(unknownRep)

    rep onSuccess {
      _ => fail("should fail")
    } handle {
      case rep: UnknownReplyCodeError =>
        assert(rep.code === 666)
      case _ => fail("should be UnknownReplyCodeError")
    }
  }
}
