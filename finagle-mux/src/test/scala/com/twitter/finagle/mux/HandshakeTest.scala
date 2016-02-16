package com.twitter.finagle.mux

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.time._
import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.{Failure, Status}
import com.twitter.finagle.transport.QueueTransport
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Await, Return}
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{OneInstancePerTest, FunSuite}
import scala.collection.immutable.Queue

@RunWith(classOf[JUnitRunner])
private class HandshakeTest extends FunSuite with OneInstancePerTest {
  import Message.{encode => enc, decode => dec}

  val clientToServer = new AsyncQueue[ChannelBuffer]
  val serverToClient = new AsyncQueue[ChannelBuffer]

  val clientTransport = new QueueTransport(
    writeq = clientToServer,
    readq = serverToClient)

  val serverTransport = new QueueTransport(
    writeq = serverToClient,
    readq = clientToServer)

  test("handshake") {
    var clientNegotiated = false
    var serverNegotiated = false

    val client = Handshake.client(
      trans = clientTransport,
      version = 0x0001,
      headers = Seq.empty,
      negotiate = (_, trans) => {
        clientNegotiated = true
        trans.map(enc, dec)
      }
    )

    val server = Handshake.server(
      trans = serverTransport,
      version = 0x0001,
      headers = identity,
      negotiate = (_, trans) => {
        serverNegotiated = true
        trans.map(enc, dec)
      }
    )

    // ensure negotiation is complete
    Await.result(client.write(Message.Tping(1)), 5.seconds)
    Await.result(server.write(Message.Rping(1)), 5.seconds)

    assert(serverNegotiated)
    assert(clientNegotiated)
  }

  test("exceptions in negotiate propagate") {
    val clientExc = new Exception("boom!")
    val client = Handshake.client(
      trans = clientTransport,
      version = 0x0001,
      headers = Seq.empty,
      negotiate = (_, trans) => {
        throw clientExc
        trans.map(enc, dec)
      }
    )

    val serverExc = new Exception("boom!")
    val server = Handshake.server(
      trans = serverTransport,
      version = 0x0001,
      headers = identity,
      negotiate = (_, trans) => {
        throw serverExc
        trans.map(enc, dec)
      }
    )

    assert(intercept[Exception] {
      Await.result(client.read(), 5.seconds)
    } == clientExc)

    assert(intercept[Exception] {
      Await.result(client.write(Message.Tping(1)), 5.seconds)
    } == clientExc)

    assert(intercept[Exception] {
      Await.result(server.read(), 5.seconds)
    } == serverExc)

    assert(intercept[Exception] {
      Await.result(server.write(Message.Rping(1)), 5.seconds)
    } == serverExc)
  }

  test("pre handshake") {
    var negotiated = false
    val client = Handshake.client(
      trans = clientTransport,
      version = 0x0001,
      headers = Nil,
      negotiate = (_, trans) => {
        negotiated = true
        trans.map(enc, dec)
      }
    )

    val f = client.write(Message.Tping(2))

    assert(dec(Await.result(clientToServer.poll(), 5.seconds)) ==
      Message.Rerr(1, "tinit check"))
    assert(!negotiated)
    assert(!f.isDefined)
  }

  test("client handshake") {
    val version = 10: Short
    val headers = Seq(
      wrappedBuffer("key".getBytes) -> wrappedBuffer("value".getBytes))
    var negotiated = false

    val client = Handshake.client(
      trans = clientTransport,
      version = version,
      headers = headers,
      negotiate = (_, trans) => {
        negotiated = true
        trans.map(enc, dec)
      }
    )

    val f = client.write(Message.Tping(2))

    assert(dec(Await.result(clientToServer.poll(), 5.seconds)) ==
      Message.Rerr(1, "tinit check"))
    assert(!negotiated)
    assert(!f.isDefined)

    serverToClient.offer(enc(Message.Rerr(1, "tinit check")))

    assert(dec(Await.result(clientToServer.poll(), 5.seconds)) ==
      Message.Tinit(1, version, headers))
    assert(!negotiated)
    assert(!f.isDefined)

    serverToClient.offer(enc(Message.Rinit(1, version, Seq.empty)))
    assert(negotiated)
    assert(f.isDefined && Await.result(f.liftToTry, 5.seconds).isReturn)
  }

  test("client fails gracefully") {
    var negotiated = false

    val client = Handshake.client(
      trans = clientTransport,
      version = 0x0001,
      headers = Seq.empty,
      negotiate = (_, trans) => {
        negotiated = true
        trans.map(enc, dec)
      }
    )

    val f = client.write(Message.Tping(2))

    assert(dec(Await.result(clientToServer.poll(), 5.seconds)) ==
      Message.Rerr(1, "tinit check"))
    assert(!negotiated)
    assert(!f.isDefined)

    serverToClient.offer(enc(Message.Rerr(1, "unexpected message Rerr")))

    assert(!negotiated)
    assert(f.isDefined && Await.result(f.liftToTry, 5.seconds).isReturn)
  }

  test("server handshake") {
    val version = 10: Short
    val hdrs = Seq(
      wrappedBuffer("key".getBytes) -> wrappedBuffer("value".getBytes))
    var negotiated = false

    val server = Handshake.server(
      trans = serverTransport,
      version = version,
      headers = _ => hdrs,
      negotiate = (_, trans) => {
        negotiated = true
        trans.map(enc, dec)
      }
    )

    clientToServer.offer(enc(Message.Tinit(1, version, Seq.empty)))

    assert(dec(Await.result(serverToClient.poll(), 5.seconds)) ==
      Message.Rinit(1, version, hdrs))
    assert(negotiated)
  }

  test("version mismatch") {
    var clientNegotiated = false
    var serverNegotiated = false

    val clientVersion: Short = 2
    val serverVersion: Short = 1

    val client = Handshake.client(
      trans = clientTransport,
      version = clientVersion,
      headers = Seq.empty,
      negotiate = (_, trans) => {
        clientNegotiated = true
        trans.map(enc, dec)
      }
    )

    val server = Handshake.server(
      trans = serverTransport,
      version = serverVersion,
      headers = identity,
      negotiate = (_, trans) => {
        serverNegotiated = true
        trans.map(enc, dec)
      }
    )

    val f0 = intercept[Failure] {
      Await.result(client.write(Message.Tping(1)), 5.seconds)
    }

    val f1 = intercept[Failure] {
      Await.result(server.write(Message.Rping(1)), 5.seconds)
    }

    val msg = s"unsupported version $clientVersion, expected $serverVersion"
    assert(f0.getMessage == msg)
    assert(f1.getMessage == msg)

    assert(!clientNegotiated && !serverNegotiated)

    Await.result(serverTransport.onClose, 5.seconds)
    Await.result(clientTransport.onClose, 5.seconds)

    assert(client.status == Status.Closed)
    assert(server.status == Status.Closed)
  }

  test("server passes non-init messages through") {
    var negotiated = false

    val server = Handshake.server(
      trans = serverTransport,
      version = 0x0001,
      headers = identity,
      negotiate = (_, trans) => {
        negotiated = true
        trans.map(enc, dec)
      }
    )

    clientToServer.offer(enc(Message.Tping(1)))
    assert(serverToClient.drain() == Return(Queue.empty))
    assert(Await.result(server.read(), 5.seconds) == Message.Tping(1))
    assert(!negotiated)
  }
}