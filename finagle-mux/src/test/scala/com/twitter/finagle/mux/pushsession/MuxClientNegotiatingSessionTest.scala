package com.twitter.finagle.mux.pushsession

import com.twitter.conversions.StorageUnitOps._
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Mux.param.{CompressionPreferences, MaxFrameSize}
import com.twitter.finagle.mux.transport.CompressionNegotiation
import com.twitter.finagle.Stack.Params
import com.twitter.finagle.pushsession.PushChannelHandle
import com.twitter.finagle.pushsession.utils.MockChannelHandle
import com.twitter.finagle.mux.Handshake.{CanTinitMsg, Headers, TinitTag}
import com.twitter.finagle.mux.Request
import com.twitter.finagle.mux.transport.Message.Tdispatch
import com.twitter.finagle.mux.transport.{Message, MuxFramer}
import com.twitter.finagle.param.OppTls
import com.twitter.finagle.ssl.OpportunisticTls
import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver}
import com.twitter.finagle.{ChannelClosedException, Failure, FailureFlags, Mux, Path, liveness}
import com.twitter.io.{Buf, ByteReader}
import com.twitter.util.{Await, Awaitable, Future, Promise, Try}
import org.scalactic.source.Position
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.Tag
import org.scalatest.funsuite.AnyFunSuite

class MuxClientNegotiatingSessionTest extends AnyFunSuite with MockitoSugar {
  import MuxClientNegotiatingSession.PushSessionQueue

  // turn off failure detector since we don't need it for these tests.
  override def test(testName: String, testTags: Tag*)(f: => Any)(implicit pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      liveness.sessionFailureDetector.let("none") { f }
    }
  }

  private[this] type Negotiator =
    (PushChannelHandle[ByteReader, Buf], Option[Headers]) => Future[MuxClientSession]

  private[this] val fragmentingParams = Mux.client.params + MaxFrameSize(2.megabytes)
  private[this] val sharedStats = new SharedNegotiationStats(NullStatsReceiver)

  private[this] val newClientSession: Negotiator = (handle, hs) => {
    new Negotiation.Client(fragmentingParams, sharedStats).negotiateAsync(handle, hs)
  }

  // Used to observe the headers received from the Server
  private[this] class HeaderObserver(params: Params) extends Negotiator {
    @volatile var observedHeaders: Option[Headers] = null

    def apply(
      handle: PushChannelHandle[ByteReader, Buf],
      hs: Option[Headers]
    ): Future[MuxClientSession] = {
      if (observedHeaders != null) sys.error("Unexpected state")
      else {
        observedHeaders = hs
        newClientSession(handle, hs)
      }
    }
  }

  private[this] def await[T](t: Awaitable[T]): T = Await.result(t, 5.seconds)

  private[this] def asByteReader(msg: Message): ByteReader =
    ByteReader(Message.encode(msg))

  private[this] def decodeClientWrite(bufs: Iterable[Buf]): Message =
    Message.decode(bufs.foldLeft(Buf.Empty)(_.concat(_)))

  private[this] def withMockHandle(
    negotiator: Negotiator,
    params: Params
  ): (MockNegotiatingChannelHandle, MuxClientNegotiatingSession, InMemoryStatsReceiver) = {
    val handle = new MockNegotiatingChannelHandle()
    val headers = Mux.Client.headers(
      params[MaxFrameSize].size,
      params[OppTls].level.getOrElse(OpportunisticTls.Off),
      params[CompressionPreferences].compressionPreferences
    )
    val stats = new InMemoryStatsReceiver
    val session = new MuxClientNegotiatingSession(
      handle = handle,
      version = 1,
      negotiator = negotiator(handle, _),
      headers = headers,
      name = "client",
      stats = stats
    )
    handle.registerSession(session)
    (handle, session, stats)
  }

  test("Will pass None to the negotiator if negotiation isn't possible") {
    val negotiate = new HeaderObserver(fragmentingParams)
    val (handle, negotiatingSession, stats) = withMockHandle(negotiate, fragmentingParams)

    assert(stats.gauges(Seq("negotiating")).apply() == 0.0f)

    val sessionF = negotiatingSession.negotiate()
    assert(stats.gauges(Seq("negotiating")).apply() == 1.0f)

    assert(Message.decode(handle.pendingWrites.dequeue().msgs.head).isInstanceOf[Message.Rerr])
    negotiatingSession.receive(asByteReader(Message.Rerr(TinitTag, "What do you mean?")))

    handle.serialExecutor.executeAll()
    await(sessionF)

    assert(!stats.gauges.contains(Seq("negotiating")))

    assert(negotiate.observedHeaders == None)

    // Make sure we installed a dispatch-read session
    assert(handle.currentSession.isInstanceOf[MuxClientSession])
  }

  test("Will pass Some(Headers) to the negotiator if returned from the server") {
    val negotiate = new HeaderObserver(fragmentingParams)
    val (handle, negotiatingSession, stats) = withMockHandle(negotiate, fragmentingParams)

    val serverHeaders = Seq(MuxFramer.Header.KeyBuf -> MuxFramer.Header.encodeFrameSize(100))
    val clientFrameSizeBuf = MuxFramer.Header.encodeFrameSize(2.megabytes.bytes.toInt)

    assert(stats.gauges(Seq("negotiating")).apply() == 0.0f)
    val sessionF = negotiatingSession.negotiate()

    assert(stats.gauges(Seq("negotiating")).apply() == 1.0f)

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

    handle.serialExecutor.executeAll()
    val service = await(sessionF.flatMap(_.asService))
    assert(!stats.gauges.contains(Seq("negotiating")))

    assert(negotiate.observedHeaders == Some(serverHeaders))

    // Make sure we installed a dispatch-read session
    assert(handle.currentSession.isInstanceOf[MuxClientSession])

    // Make sure we are fragmenting the messages

    // Server only wants 100 byte chunks, so make the message at least 150 bytes
    val decoder = new FragmentDecoder(NullStatsReceiver)
    val data = Buf.ByteArray((0 until 150).map(_.toByte): _*)

    service.apply(Request(Path(), data))
    handle.serialExecutor.executeAll()

    // Chunk 1 shouldn't be a complete message
    assert(
      decoder
        .decode(ByteReader(handle.dequeAndCompleteWrite().foldLeft(Buf.Empty)(_.concat(_)))) == null
    )

    handle.serialExecutor.executeAll()

    // Chunk 2 should be a complete message
    decoder.decode(
      ByteReader(handle.dequeAndCompleteWrite().foldLeft(Buf.Empty)(_.concat(_)))) match {
      case Tdispatch(_, _, Path(), _, req) => req == data
      case other => fail(s"unexpected message: $other")
    }
  }

  test("Marker Rerr followed by non-Rinit fails new session") {
    val (handle, negotiatingSession, stats) = withMockHandle((_, _) => ???, fragmentingParams)
    assert(stats.gauges(Seq("negotiating")).apply() == 0.0f)
    val sessionF = negotiatingSession.negotiate()

    assert(stats.gauges(Seq("negotiating")).apply() == 1.0f)

    assert(decodeClientWrite(handle.dequeAndCompleteWrite()).isInstanceOf[Message.Rerr])
    negotiatingSession.receive(asByteReader(Message.Rerr(TinitTag, CanTinitMsg)))

    decodeClientWrite(handle.dequeAndCompleteWrite()) match {
      case Message.Tinit(_, _, _) => // sweet
      case other => fail(s"Unexpected message: $other")
    }

    negotiatingSession.receive(asByteReader(Message.Rerr(1, "what....")))

    intercept[Failure] {
      await(sessionF)
    }

    assert(!stats.gauges.contains(Seq("negotiating")))
  }

  test("Handle onClose failure cancels the handshake") {
    val negotiate: Negotiator =
      (handle, hs) =>
        Future.value(
          new Negotiation.Client(fragmentingParams, sharedStats).negotiate(handle, hs)
        )

    val (handle, negotiatingSession, stats) = withMockHandle(negotiate, fragmentingParams)
    assert(stats.gauges(Seq("negotiating")).apply() == 0.0f)
    val sessionF = negotiatingSession.negotiate()
    assert(stats.gauges(Seq("negotiating")).apply() == 1.0f)
    handle.close()
    val exc = new Exception("boom")
    handle.onClosePromise.setException(exc)

    val observedExc = intercept[Exception] {
      await(sessionF)
    }

    assert(exc eq observedExc)
    assert(!stats.gauges.contains(Seq("negotiating")))
  }

  test("Handle normal onClose cancels the handshake") {
    val (handle, negotiatingSession, stats) = withMockHandle(newClientSession, fragmentingParams)
    assert(stats.gauges(Seq("negotiating")).apply() == 0.0f)
    val sessionF = negotiatingSession.negotiate()
    assert(stats.gauges(Seq("negotiating")).apply() == 1.0f)
    handle.close()
    handle.onClosePromise.setDone()

    val ex = intercept[ChannelClosedException] {
      await(sessionF)
    }

    assert(ex.isFlagged(FailureFlags.Retryable))
    assert(!stats.gauges.contains(Seq("negotiating")))
  }

  test("negotiation failure") {
    val exc = new Exception("boom")
    val negotiate: Negotiator = (_, _) => Future.exception(exc)

    val (handle, negotiatingSession, stats) = withMockHandle(negotiate, fragmentingParams)
    assert(stats.gauges(Seq("negotiating")).apply() == 0.0f)
    val sessionF = negotiatingSession.negotiate()
    assert(stats.gauges(Seq("negotiating")).apply() == 1.0f)

    assert(decodeClientWrite(handle.dequeAndCompleteWrite()).isInstanceOf[Message.Rerr])
    negotiatingSession.receive(asByteReader(Message.Rerr(TinitTag, CanTinitMsg)))

    decodeClientWrite(handle.dequeAndCompleteWrite()) match {
      case Message.Tinit(_, _, _) => // sweet
      case other => fail(s"Unexpected message: $other")
    }

    negotiatingSession.receive(asByteReader(Message.Rinit(TinitTag, 1, Seq.empty)))

    handle.serialExecutor.executeAll()
    val e = intercept[Exception] {
      await(sessionF)
    }

    assert(e == exc)
    assert(!stats.gauges.contains(Seq("negotiating")))
  }

  test("can be interrupted") {
    val (handle, negotiatingSession, stats) = withMockHandle(newClientSession, fragmentingParams)
    assert(stats.gauges(Seq("negotiating")).apply() == 0.0f)
    val sessionF = negotiatingSession.negotiate()
    assert(stats.gauges(Seq("negotiating")).apply() == 1.0f)

    val raised = new Exception
    sessionF.raise(raised)

    assert(handle.closedCalled)
    handle.onClosePromise.setDone()

    val ex = intercept[Failure] {
      await(sessionF)
    }
    assert(ex.isFlagged(FailureFlags.Retryable))
    assert(ex.cause == Some(raised))
    assert(!stats.gauges.contains(Seq("negotiating")))
  }

  test("messages are queued while negotiator is outstanding") {
    val p = Promise[MuxClientSession]
    @volatile var setNegotiatePromise: () => Unit = null
    val negotiate: Negotiator = (handle, hs) => {
      val n = new Negotiation.Client(fragmentingParams, sharedStats).negotiate(handle, hs)
      setNegotiatePromise = () => p.setValue(n)
      p
    }

    val (handle, negotiatingSession, stats) = withMockHandle(negotiate, fragmentingParams)
    assert(stats.gauges(Seq("negotiating")).apply() == 0.0f)

    val sessionF = negotiatingSession.negotiate()
    negotiatingSession.receive(asByteReader(Message.Rerr(TinitTag, "What do you mean?")))
    assert(stats.gauges(Seq("negotiating")).apply() == 1.0f)
    assert(!sessionF.isDefined)

    assert(handle.currentSession.isInstanceOf[PushSessionQueue])
    val qSession = handle.currentSession.asInstanceOf[PushSessionQueue]

    assert(stats.gauges(Seq("negotiating_queue_size")).apply() == 0)
    for (_ <- 0 until 1) { qSession.receive(asByteReader(Message.Tping(tag = 10))) }
    assert(stats.gauges(Seq("negotiating_queue_size")).apply() == 1)

    setNegotiatePromise()
    handle.serialExecutor.executeAll()
    await(sessionF)
    assert(!stats.gauges.contains(Seq("negotiating")))
    assert(!stats.gauges.contains(Seq("negotiating_queue_size")))

    assert(handle.currentSession.isInstanceOf[MuxClientSession])
  }

  test("Will enable compression when compression is negotiated") {
    val negotiate = new HeaderObserver(fragmentingParams)
    val (handle, negotiatingSession, stats) = withMockHandle(negotiate, fragmentingParams)
    val fmt = CompressionNegotiation.CompressionFormats(Some("foo"), Some("foo"))

    val serverHeaders = Seq(
      MuxFramer.Header.KeyBuf -> MuxFramer.Header.encodeFrameSize(100),
      CompressionNegotiation.ServerHeader.KeyBuf -> CompressionNegotiation.ServerHeader.encode(fmt)
    )
    val clientFrameSizeBuf = MuxFramer.Header.encodeFrameSize(2.megabytes.bytes.toInt)

    assert(stats.gauges(Seq("negotiating")).apply() == 0.0f)
    val sessionF = negotiatingSession.negotiate()

    assert(stats.gauges(Seq("negotiating")).apply() == 1.0f)

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

    assert(!handle.compressionOn && !handle.decompressionOn)
    negotiatingSession.receive(asByteReader(Message.Rinit(TinitTag, 1, serverHeaders)))

    handle.serialExecutor.executeAll()
    val service = await(sessionF.flatMap(_.asService))
    assert(!stats.gauges.contains(Seq("negotiating")))

    assert(negotiate.observedHeaders == Some(serverHeaders))

    assert(handle.compressionOn && handle.decompressionOn)
  }
}

class MockNegotiatingChannelHandle
    extends MockChannelHandle[ByteReader, Buf](
      null
    )
    with NegotiatingHandle {
  var tlsOn = false
  var compressionOn = false
  var decompressionOn = false

  def turnOnTls(onHandshakeComplete: Try[Unit] => Unit): Unit = {
    tlsOn = true
  }
  def turnOnCompression(format: String): Unit = {
    compressionOn = true
  }
  def turnOnDecompression(format: String): Unit = {
    decompressionOn = true
  }
}
