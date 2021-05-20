package com.twitter.finagle.mux.pushsession

import com.twitter.finagle.pushsession.utils.MockChannelHandle
import com.twitter.finagle.{Dtab, Mux, Path, Service, Status}
import com.twitter.finagle.pushsession.{
  PushChannelHandle,
  PushSession,
  RefPushSession,
  SentinelSession
}
import com.twitter.finagle.mux.Handshake.Headers
import com.twitter.finagle.mux.{Handshake, Request, Response}
import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.mux.transport.Message.{Rerr, Rinit, Tdispatch, Tinit}
import com.twitter.io.{Buf, ByteReader}
import com.twitter.util.{Future, Promise, Time, Timer}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import scala.collection.mutable
import org.scalatest.funsuite.AnyFunSuite

class MuxServerNegotiatorTest extends AnyFunSuite with Eventually with IntegrationPatience {

  private val data: Buf = Buf.ByteArray.Owned((0 until 1024).map(_.toByte).toArray)

  private class MockSession(
    handle: PushChannelHandle[ByteReader, Buf],
    val headers: Option[Headers])
      extends PushSession[ByteReader, Buf](handle) {

    val receivedMessages = new mutable.Queue[Message]()

    val closeP = Promise[Unit]()

    def receive(message: ByteReader): Unit = {
      receivedMessages += Message.decode(message)
    }

    def status: Status = Status.Open

    def close(deadline: Time): Future[Unit] = closeP
  }

  private abstract class Ctx {
    private class MockMuxChannelHandle(underlying: PushChannelHandle[ByteReader, Buf])
        extends MuxChannelHandle(
          underlying = underlying,
          ch = null, // only used in overridden method `sendAndForgetNow`
          params = params
        ) {
      override def sendNowAndForget(buf: Buf): Unit = sendAndForget(buf)
    }

    def params = Mux.server.params

    var resolvedSession: MockSession = null

    lazy val localHeaders: Headers => Headers = hs =>
      Seq(Buf.Utf8("local") -> Buf.Utf8("header")) ++ hs

    var serviceClosed = false

    lazy val service: Service[Request, Response] = new Service[Request, Response] {
      def apply(req: Request) = ??? // Not actually used.

      override def close(deadline: Time): Future[Unit] = {
        serviceClosed = true
        super.close(deadline)
      }
    }

    lazy val handle: MockChannelHandle[ByteReader, Buf] = new MockChannelHandle[ByteReader, Buf]()

    def negotiate(
      service: Service[Request, Response],
      headers: Option[Headers]
    ): PushSession[ByteReader, Buf] = {
      resolvedSession = new MockSession(handle, headers)
      resolvedSession
    }

    private val muxHandle = new MockMuxChannelHandle(handle)
    private val ref =
      new RefPushSession[ByteReader, Buf](muxHandle, SentinelSession[ByteReader, Buf](muxHandle))
    MuxServerNegotiator.build(
      ref = ref,
      handle = muxHandle,
      service = service,
      makeLocalHeaders = localHeaders,
      negotiate = negotiate,
      timer = Timer.Nil
    )

    // Alias with less refined type
    def session: PushSession[ByteReader, Buf] = ref

    def receiveMessage(msg: Message): Unit = {
      val br = ByteReader(Message.encode(msg))
      session.receive(br)
    }

    def popSentMessage(): Message = {
      val message = handle.pendingWrites.dequeue()
      message.completeSuccess()
      Message.decode(message.msgs.foldLeft(Buf.Empty)(_ concat _))
    }
  }

  test("no handshake") {
    new Ctx {
      val dispatch = Tdispatch(2, Nil, Path.empty, Dtab.empty, data)
      receiveMessage(dispatch)

      assert(resolvedSession.receivedMessages.dequeue() == dispatch)
      assert(resolvedSession.headers == None)
    }
  }

  test("standard handshake") {
    new Ctx {
      val handshakeRerr = Rerr(Handshake.TinitTag, Handshake.CanTinitMsg)
      receiveMessage(handshakeRerr)
      assert(resolvedSession == null)
      assert(popSentMessage() == handshakeRerr) // we echo the Rerr

      val headers = Seq(Buf.Utf8("hello") -> Buf.Utf8("world"))
      receiveMessage(Tinit(1, 1, headers))

      assert(popSentMessage() == Rinit(1, 1, localHeaders(headers)))
      handle.serialExecutor.executeAll()
      assert(resolvedSession.headers == Some(headers))
    }
  }

  test("Exception throwing handshake") {
    new Ctx {
      override def negotiate(
        service: Service[Request, Response],
        headers: Option[Headers]
      ): PushSession[ByteReader, Buf] = {
        throw new Exception("lolz")
      }

      val handshakeRerr = Rerr(Handshake.TinitTag, Handshake.CanTinitMsg)
      receiveMessage(handshakeRerr)
      assert(resolvedSession == null)
      assert(popSentMessage() == handshakeRerr) // we echo the Rerr

      receiveMessage(Tinit(1, 1, Seq.empty))
      // Should have been sent before negotiation happened
      assert(popSentMessage() == Rinit(1, 1, localHeaders(Seq.empty)))
      handle.serialExecutor.executeAll()

      // Shouldn't be closed until the Rerr is flushed
      assert(!handle.closedCalled)
      assert(!serviceClosed)

      val Rerr(1, _) = popSentMessage()

      eventually {
        // Make sure we closed both the handle and the service
        assert(handle.closedCalled)
        assert(serviceClosed)
      }
    }
  }

  test("handle closing should close the service") {
    new Ctx {
      assert(session.status == Status.Open)
      handle.onClosePromise.setDone()

      eventually {
        assert(serviceClosed)
        assert(handle.closedCalled)
      }
    }
  }
}
