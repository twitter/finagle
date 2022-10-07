package com.twitter.finagle.http2.transport.client

import com.twitter.util.Await
import com.twitter.util.Awaitable
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Promise
import com.twitter.util.Time
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.OneInstancePerTest
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class DeferredCloseSessionTest extends AnyFunSuite with MockitoSugar with OneInstancePerTest {

  private[this] def await[T](t: Awaitable[T]): T =
    Await.result(t, Duration.fromSeconds(5))

  private[this] val latch = Promise[Unit]()
  private[this] val underlying = mock[ClientSession]
  when(underlying.close(any(classOf[Time]))).thenReturn(Future.Unit)

  private[this] val deferredCloseSession = new DeferredCloseSession(underlying, latch)

  test("close post-latch") {
    latch.setDone()
    await(deferredCloseSession.close())
    verify(underlying, times(1)).close(any(classOf[Time]))
  }

  test("close pre-latch") {
    val f = deferredCloseSession.close()
    assert(!f.isDefined)
    verify(underlying, times(0)).close(any(classOf[Time]))
    latch.setDone()
    await(f)
    verify(underlying, times(1)).close(any(classOf[Time]))
  }
}
