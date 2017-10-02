package com.twitter.finagle.mux.exp.pushsession

import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.finagle.{ChannelClosedException, Path, liveness}
import com.twitter.finagle.Mux.param.MaxFrameSize
import com.twitter.finagle.Stack.Params
import com.twitter.finagle.exp.pushsession.{MockChannelHandle, PushChannelHandle}
import com.twitter.finagle.mux.Handshake.{CanTinitMsg, Headers, TinitTag}
import com.twitter.finagle.mux.Request
import com.twitter.finagle.mux.transport.Message.Tdispatch
import com.twitter.finagle.mux.transport.{Message, MuxFramer}
import com.twitter.io.{Buf, ByteReader}
import com.twitter.util.{Await, Awaitable}
import org.scalactic.source.Position
import org.scalatest.{FunSuite, Tag}
import org.scalatest.mockito.MockitoSugar

class MuxClientNegotiatingSessionTest extends FunSuite with MockitoSugar {

  // turn off failure detector since we don't need it for these tests.
  override def test(testName: String, testTags: Tag*)(f: => Any)(implicit pos: Position) {
    super.test(testName, testTags: _*) {
      liveness.sessionFailureDetector.let("none") { f }
    }
  }

  type ChannelHandleT = MockChannelHandle[ByteReader, Buf]
  type Negotiator = (PushChannelHandle[ByteReader, Buf], Option[Headers]) => MuxClientSession

  // Used to observe the headers received from the Server
  private class HeaderObserver(params: Params) extends Negotiator {
    @volatile
    var observedHeaders: Option[Headers] = null

    def apply(
      handle: PushChannelHandle[ByteReader, Buf],
      hs: Option[Headers]
    ): MuxClientSession = {
      if (observedHeaders != null) sys.error("Unexpected state")
      else {
        observedHeaders = hs
        MuxPush.negotiateClientSession(handle, params, hs)
      }
    }
  }

  private[this] val fragmentingParams = MuxPush.client.params + MaxFrameSize(2.megabytes)

  private[this] def await[T](t: Awaitable[T]): T = Await.result(t, 5.seconds)

  private[this] def asByteReader(msg: Message): ByteReader =
    ByteReader(Message.encode(msg))

  private[this] def decodeClientWrite(bufs: Iterable[Buf]): Message =
    Message.decode(bufs.foldLeft(Buf.Empty)(_.concat(_)))

  private def withMockHandle(negotiator: Negotiator, params: Params): (ChannelHandleT, MuxClientNegotiatingSession) = {
    val handle = new MockChannelHandle[ByteReader, Buf](null)
    val session = new MuxClientNegotiatingSession(
      handle, 1, negotiator(handle, _), params[MaxFrameSize].size, "client")
    handle.registerSession(session)
    handle -> session
  }

  test("Will pass None to the negotiator if negotiation isn't possible") {
    val negotiate = new HeaderObserver(fragmentingParams)
    val (handle, negotiatingSession) = withMockHandle(negotiate, fragmentingParams)

    val sessionF = negotiatingSession.negotiate()
    assert(Message.decode(handle.pendingWrites.dequeue().msgs.head).isInstanceOf[Message.Rerr])
    negotiatingSession.receive(asByteReader(Message.Rerr(TinitTag, "What do you mean?")))

    await(sessionF)

    assert(negotiate.observedHeaders == None)
    // Make sure we installed a dispatch-read session
    assert(handle.currentSession.isInstanceOf[MuxClientSession])
  }

  test("Will pass Some(Headers) to the negotiator if returned from the server") {
    val negotiate = new HeaderObserver(fragmentingParams)
    val (handle, negotiatingSession) = withMockHandle(negotiate, fragmentingParams)

    val serverHeaders = Seq(MuxFramer.Header.KeyBuf -> MuxFramer.Header.encodeFrameSize(100))
    val clientFrameSizeBuf = MuxFramer.Header.encodeFrameSize(2.megabytes.bytes.toInt)
    val sessionF = negotiatingSession.negotiate()

    assert(decodeClientWrite(handle.dequeAndCompleteWrite()).isInstanceOf[Message.Rerr])

    negotiatingSession.receive(asByteReader(Message.Rerr(TinitTag, CanTinitMsg)))

    // Make sure the client sent its max frame size header
    decodeClientWrite(handle.dequeAndCompleteWrite()) match {
      case Message.Tinit(_, _, hs) =>
        val frameSizeValue = hs.collectFirst { case (MuxFramer.Header.KeyBuf, v) => v }
        assert(frameSizeValue.get == clientFrameSizeBuf)

      case other =>
        fail(s"Unexpected message: $other")
    }

    negotiatingSession.receive(asByteReader(Message.Rinit(TinitTag, 1, serverHeaders)))

    val service = await(sessionF.flatMap(_.asService))

    assert(negotiate.observedHeaders == Some(serverHeaders))

    // Make sure we installed a dispatch-read session
    assert(handle.currentSession.isInstanceOf[MuxClientSession])

    // Make sure we are fragmenting the messages

    // Server only wants 100 byte chunks, so make the message at lest 150 bytes
    val decoder = new FragmentDecoder
    val data = Buf.ByteArray((0 until 150).map(_.toByte):_*)

    service.apply(Request(Path(), data))
    handle.serialExecutor.executeAll()

    // Chunk 1 shouldn't be a complete message
    assert(decoder.decode(
      ByteReader(handle.dequeAndCompleteWrite().foldLeft(Buf.Empty)(_.concat(_)))) == null)

    handle.serialExecutor.executeAll()

    // Chunk 2 should be a complete message
    decoder.decode(
      ByteReader(handle.dequeAndCompleteWrite().foldLeft(Buf.Empty)(_.concat(_)))) match {
      case Tdispatch(_, _, Path(), _, req) => req == data
      case other => fail(s"unexpected message: $other")
    }
  }

  test("Handle onClose failure cancels the handshake") {
    def negotiate(handle: PushChannelHandle[ByteReader, Buf], hs: Option[Headers]): MuxClientSession =
      MuxPush.negotiateClientSession(handle, fragmentingParams, hs)

    val (handle, negotiatingSession) = withMockHandle(negotiate, fragmentingParams)
    val sessionF = negotiatingSession.negotiate()
    handle.close()
    val exc = new Exception("boom")
    handle.onClosePromise.setException(exc)

    val observedExc = intercept[Exception] {
      await(sessionF)
    }

    assert(exc eq observedExc)
  }

  test("Handle normal onClose cancels the handshake") {
    def negotiate(handle: PushChannelHandle[ByteReader, Buf], hs: Option[Headers]): MuxClientSession =
      MuxPush.negotiateClientSession(handle, fragmentingParams, hs)

    val (handle, negotiatingSession) = withMockHandle(negotiate, fragmentingParams)
    val sessionF = negotiatingSession.negotiate()
    handle.close()
    handle.onClosePromise.setDone()

    intercept[ChannelClosedException] {
      await(sessionF)
    }
  }
}
