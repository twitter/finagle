package com.twitter.finagle.http2.transport

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.finagle.{Stack, Status}
import com.twitter.util._
import java.util.concurrent.atomic.AtomicInteger
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatestplus.mockito.MockitoSugar

class Http2NegotiatingTransporterTest
    extends FunSuite
    with MockitoSugar
    with Eventually
    with IntegrationPatience {
  def await[T](f: Future[T], wait: Duration = 1.second) =
    Await.result(f, wait)

  List().contains(null)

  private abstract class TestableNegotiatingTransporter(
    params: Stack.Params,
    http1Transporter: Transporter[Any, Any, TransportContext],
    fallbackToHttp11WhileNegotiating: Boolean = true)
      extends Http2NegotiatingTransporter(
        params,
        http1Transporter,
        fallbackToHttp11WhileNegotiating
      ) {
    final def cached: Boolean = connectionCached
  }

  test("caches transports") {
    val http1Transporter = mock[Transporter[Any, Any, TransportContext]]
    val transporter = new TestableNegotiatingTransporter(Stack.Params.empty, http1Transporter) {
      protected def attemptUpgrade(
      ): (Future[Option[ClientSession]], Future[Transport[Any, Any]]) = {
        val clientSession = mock[ClientSession]
        val transport = mock[Transport[Any, Any]]
        Future.value(Some(clientSession)) -> Future.value(transport)
      }
    }

    transporter()
    eventually { assert(transporter.cached) }
  }

  test("decaches resolved underlying session when closed") {
    val http1Transporter = mock[Transporter[Any, Any, TransportContext]]
    val clientSession = new ClientSession {
      def newChildTransport(): Future[Transport[Any, Any]] = ???
      def status: Status = ???
      def close(deadline: Time): Future[Unit] = Future.Done
    }

    val transporter = new TestableNegotiatingTransporter(Stack.Params.empty, http1Transporter) {
      protected def attemptUpgrade(
      ): (Future[Option[ClientSession]], Future[Transport[Any, Any]]) = {
        val transport = mock[Transport[Any, Any]]
        Future.value(Some(clientSession)) -> Future.value(transport)
      }
    }

    val tf = transporter()
    eventually { assert(transporter.cached) }

    await(tf)
    await(transporter.close(Time.Bottom))
    assert(!transporter.cached)
  }

  test("decaches unresolved underlying session when closed") {
    val http1Transporter = mock[Transporter[Any, Any, TransportContext]]
    val p = Promise[Option[ClientSession]]()
    val transporter = new TestableNegotiatingTransporter(Stack.Params.empty, http1Transporter) {
      protected def attemptUpgrade(
      ): (Future[Option[ClientSession]], Future[Transport[Any, Any]]) = {
        val transport = mock[Transport[Any, Any]]
        p -> Future.value(transport)
      }
    }

    val tf = transporter()
    eventually { assert(transporter.cached) }

    await(tf)
    await(transporter.close(Time.Bottom))
    // We should be interrupting the resolution of the session.
    assert(!transporter.cached)
    eventually { assert(p.isInterrupted.isDefined) }
  }

  test(
    "uses http11 for the second outstanding transport pre-upgrade if fallbackToHttp11WhileNegotiating=true"
  ) {
    val http1Transporter = mock[Transporter[Any, Any, TransportContext]]
    val http1Transport = mock[Transport[Any, Any]]
    when(http1Transporter.apply()).thenReturn(Future.value(http1Transport))
    when(http1Transport.status).thenReturn(Status.Open)

    val transport = mock[Transport[Any, Any]]
    val transporter =
      new TestableNegotiatingTransporter(Stack.Params.empty, http1Transporter, true) {
        protected def attemptUpgrade(
        ): (Future[Option[ClientSession]], Future[Transport[Any, Any]]) = {
          Future.never -> Future.value(transport)
        }
      }

    val tf = transporter()
    eventually { assert(transporter.cached) }

    assert(tf.poll == Some(Return(transport)))

    val http11Trans = await(transporter())
    verify(http1Transport, times(0)).status
    assert(http11Trans.status == Status.Open)
    // Make sure we used the http1 transport
    verify(http1Transport, times(1)).status
  }

  test("waits for the singleton if fallbackToHttp11WhileNegotiating=false") {
    // This should be unused for this test
    val http1Transporter = mock[Transporter[Any, Any, TransportContext]]

    val singleton = Promise[Option[ClientSession]]()
    val transporter =
      new TestableNegotiatingTransporter(Stack.Params.empty, http1Transporter, false) {
        protected def attemptUpgrade(
        ): (Future[Option[ClientSession]], Future[Transport[Any, Any]]) = {
          singleton -> singleton.flatMap {
            case Some(session) => session.newChildTransport()
            case None => fail("Expected a ClientSession, found None.")
          }
        }
      }

    transporter()
    eventually { assert(transporter.cached) }
    transporter()

    val clientSession = mock[ClientSession]
    when(clientSession.status).thenReturn(Status.Open)
    when(clientSession.newChildTransport()).thenReturn(Future.never)

    singleton.setValue(Some(clientSession))

    eventually {
      verify(clientSession, times(2)).newChildTransport()
      verify(http1Transporter, times(0)).apply()
    }
  }

  test("reuses the http2 transporter postupgrade") {
    val http1Transporter = mock[Transporter[Any, Any, TransportContext]]
    val clientSession = mock[ClientSession]
    val sessionTransporter = mock[Transport[Any, Any]]

    when(clientSession.newChildTransport()).thenReturn(Future.value(sessionTransporter))
    val transport = mock[Transport[Any, Any]]
    val transporter = new TestableNegotiatingTransporter(Stack.Params.empty, http1Transporter) {
      protected def attemptUpgrade(
      ): (Future[Option[ClientSession]], Future[Transport[Any, Any]]) = {
        Future.value(Some(clientSession)) -> Future.value(transport)
      }
    }

    val tf = transporter()
    eventually { assert(transporter.cached) }

    assert(tf.poll == Some(Return(transport)))
    assert(transporter().poll == Some(Return(sessionTransporter)))
  }

  test("uses the http11 transporter post rejection") {
    val http1Transporter = mock[Transporter[Any, Any, TransportContext]]
    val http1Transport = mock[Transport[Any, Any]]
    when(http1Transporter.apply()).thenReturn(Future.value(http1Transport))

    val firstTransport = mock[Transport[Any, Any]]

    val transporter = new TestableNegotiatingTransporter(Stack.Params.empty, http1Transporter) {
      protected def attemptUpgrade(
      ): (Future[Option[ClientSession]], Future[Transport[Any, Any]]) = {
        Future.value(None) -> Future.value(firstTransport)
      }
    }

    val tf = transporter()
    eventually { assert(transporter.cached) }
    assert(tf.poll == Some(Return(firstTransport)))
    assert(await(transporter()) == http1Transport)
  }

  test("marks outstanding transports dead after a successful upgrade") {
    val http1Transporter = mock[Transporter[Any, Any, TransportContext]]
    val http1Transport = mock[Transport[Any, Any]]
    when(http1Transporter.apply()).thenReturn(Future.value(http1Transport))
    when(http1Transport.status).thenReturn(Status.Open)
    val clientSession = mock[ClientSession]
    when(clientSession.status).thenReturn(Status.Open)
    val sessionTransporter = mock[Transport[Any, Any]]
    when(clientSession.newChildTransport()).thenReturn(Future.value(sessionTransporter))

    val p = Promise[Option[ClientSession]]()
    val transport = mock[Transport[Any, Any]]
    val transporter = new TestableNegotiatingTransporter(Stack.Params.empty, http1Transporter) {
      protected def attemptUpgrade(
      ): (Future[Option[ClientSession]], Future[Transport[Any, Any]]) = {
        p -> Future.value(transport)
      }
    }

    val tf = transporter()
    eventually { assert(transporter.cached) }

    assert(tf.poll == Some(Return(transport)))

    val http11Trans = await(transporter())
    assert(http11Trans.status == Status.Open)
    verify(http1Transport, times(1)).status
    p.setValue(Some(clientSession))
    assert(http11Trans.status == Status.Closed)
    verify(http1Transport, times(2)).status
  }

  test("keeps outstanding transports alive after a failed upgrade") {
    val http1Transporter = mock[Transporter[Any, Any, TransportContext]]
    val http1Transport = mock[Transport[Any, Any]]
    when(http1Transporter.apply()).thenReturn(Future.value(http1Transport))
    when(http1Transport.status).thenReturn(Status.Open)

    val p = Promise[Option[ClientSession]]()
    val transport = mock[Transport[Any, Any]]
    val transporter = new TestableNegotiatingTransporter(Stack.Params.empty, http1Transporter) {
      protected def attemptUpgrade(
      ): (Future[Option[ClientSession]], Future[Transport[Any, Any]]) = {
        p -> Future.value(transport)
      }
    }

    val tf = transporter()
    eventually { assert(transporter.cached) }

    assert(tf.poll == Some(Return(transport)))

    val http11Trans = await(transporter())
    assert(http11Trans.status == Status.Open)
    verify(http1Transport, times(1)).status
    p.setValue(None)
    assert(http11Trans.status == Status.Open)
    verify(http1Transport, times(2)).status
  }

  test("doesn't mark outstanding transports dead after a failed connect attempt") {
    val http1Transporter = mock[Transporter[Any, Any, TransportContext]]
    val http1Transport = mock[Transport[Any, Any]]
    when(http1Transporter.apply()).thenReturn(Future.value(http1Transport))
    when(http1Transport.status).thenReturn(Status.Open)

    val p = Promise[Option[ClientSession]]()
    val transport = mock[Transport[Any, Any]]
    val transporter = new TestableNegotiatingTransporter(Stack.Params.empty, http1Transporter) {
      protected def attemptUpgrade(
      ): (Future[Option[ClientSession]], Future[Transport[Any, Any]]) = {
        p -> Future.value(transport)
      }
    }

    val tf = transporter()
    eventually { assert(transporter.cached) }

    assert(tf.poll == Some(Return(transport)))

    val http11Trans = await(transporter())
    assert(http11Trans.status == Status.Open)
    verify(http1Transport, times(1)).status
    p.setException(new Exception())
    assert(http11Trans.status == Status.Open)
    verify(http1Transport, times(2)).status
  }

  test("can try to establish a connection again if a connection failed") {
    val http1Transporter = mock[Transporter[Any, Any, TransportContext]]
    val http1Transport = mock[Transport[Any, Any]]
    when(http1Transporter.apply()).thenReturn(Future.value(http1Transport))
    when(http1Transport.status).thenReturn(Status.Open)
    val transport = mock[Transport[Any, Any]]

    val upgradeCalls = new AtomicInteger(0)
    val transporter = new TestableNegotiatingTransporter(Stack.Params.empty, http1Transporter) {
      protected def attemptUpgrade(
      ): (Future[Option[ClientSession]], Future[Transport[Any, Any]]) = {
        upgradeCalls.incrementAndGet()
        val clientSession = mock[ClientSession]
        when(clientSession.status).thenReturn(Status.Closed)
        when(clientSession.newChildTransport()).thenReturn(Future.exception(new Exception()))
        Future.value(Some(clientSession)) -> Future.value(transport)
      }
    }

    assert(transporter().poll == Some(Return(transport)))
    assert(upgradeCalls.get == 1)

    assert(transporter().poll == Some(Return(transport)))
    assert(upgradeCalls.get == 2)
  }

  test("session status is bubbled up correctly") {
    val http1Transporter = mock[Transporter[Any, Any, TransportContext]]
    val transport = mock[Transport[Any, Any]]
    val clientSession = mock[ClientSession]
    when(clientSession.status).thenReturn(Status.Open)

    val transporter = new TestableNegotiatingTransporter(Stack.Params.empty, http1Transporter) {
      protected def attemptUpgrade(
      ): (Future[Option[ClientSession]], Future[Transport[Any, Any]]) = {
        when(clientSession.newChildTransport()).thenReturn(Future.exception(new Exception()))
        Future.value(Some(clientSession)) -> Future.value(transport)
      }
    }

    // Initial status is Open
    assert(transporter.transporterStatus == Status.Open)

    // After a session resolves and has status Open, we should still be Open.
    assert(transporter().poll == Some(Return(transport)))
    assert(transporter.transporterStatus == Status.Open)

    // Now set it to busy, and the session status should reflect this.
    when(clientSession.status).thenReturn(Status.Busy)
    assert(transporter.transporterStatus == Status.Busy)

    // When the session is Closed, we are going to evict it so we go back to an Open state.
    when(clientSession.status).thenReturn(Status.Closed)
    assert(transporter.transporterStatus == Status.Open)
  }
}
