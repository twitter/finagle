package com.twitter.finagle.http2.transport.client

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.transport.TransportProxy
import com.twitter.util.Await
import com.twitter.util.Future
import com.twitter.util.Time
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyInt
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class RefTransportTest extends AnyFunSuite with MockitoSugar {

  // wrapping the transport in a thin proxy provides a correct map
  // implementation
  private[this] def wrap(trans: Transport[Int, Int]): Transport[Int, Int] =
    new TransportProxy(trans) {
      override def write(in: Int): Future[Unit] = trans.write(in)
      override def read(): Future[Int] = trans.read()
    }

  // returns None if it updates and hasn't been closed yet
  // return Some[Boolean] if it has already been closed.  the boolean indicates whether
  // close was then called on it or not.
  private[this] def updateMap(
    trans: RefTransport[Int, Int],
    left: Int => Int,
    right: Int => Int
  ): Option[Boolean] = {
    var closed = false
    val updated = trans.update { transport =>
      val underlying = transport.map(left, right)
      new TransportProxy(underlying) {
        override def read(): Future[Int] = underlying.read()
        override def write(msg: Int): Future[Unit] = underlying.write(msg)
        override def close(deadline: Time): Future[Unit] = {
          closed = true
          underlying.close(deadline)
        }
      }
    }
    if (updated) None else Some(closed)
  }

  test("RefTransport proxies to underlying transport") {
    val trans = mock[Transport[Int, Int]]
    when(trans.write(anyInt)).thenReturn(Future.Done)
    when(trans.read()).thenReturn(Future.value(1))
    when(trans.onClose).thenReturn(Future.never)
    val refTrans = new RefTransport(wrap(trans))

    assert(Await.result(refTrans.read(), 5.seconds) == 1)
    refTrans.write(7)
    verify(trans).write(7)
  }

  test("RefTransport proxies to underlying transport via a lens") {
    val trans = mock[Transport[Int, Int]]
    when(trans.write(anyInt)).thenReturn(Future.Done)
    when(trans.read()).thenReturn(Future.value(1))
    when(trans.onClose).thenReturn(Future.never)
    val refTrans = new RefTransport(wrap(trans))

    assert(updateMap(refTrans, _ * 2, _ * 3).isEmpty)

    assert(Await.result(refTrans.read(), 5.seconds) == 3)
    refTrans.write(7)
    verify(trans).write(14)
  }

  test("RefTransport proxies to underlying transport via a lens which can switch") {
    val trans = mock[Transport[Int, Int]]
    when(trans.write(anyInt)).thenReturn(Future.Done)
    when(trans.read()).thenReturn(Future.value(1))
    when(trans.onClose).thenReturn(Future.never)
    val refTrans = new RefTransport(wrap(trans))

    assert(updateMap(refTrans, _ * 2, _ * 3).isEmpty)

    assert(Await.result(refTrans.read(), 5.seconds) == 3)

    updateMap(refTrans, _ * 8, _ * 1)
    refTrans.write(7)
    verify(trans).write(56)
  }

  test("RefTransport will immediately close the updated transport after closing") {
    val trans = mock[Transport[Int, Int]]
    when(trans.write(anyInt)).thenReturn(Future.Done)
    when(trans.read()).thenReturn(Future.value(1))
    when(trans.onClose).thenReturn(Future.never)
    when(trans.close(any[Time])).thenReturn(Future.never)
    val refTrans = new RefTransport(wrap(trans))
    refTrans.close()

    assert(updateMap(refTrans, _ * 2, _ * 2) == Some(true))

    assert(Await.result(refTrans.read(), 5.seconds) == 2)
    refTrans.write(7)
    verify(trans).write(14)
  }

  test("RefTransport will immediately close after the underlying transport is closed") {
    val trans = mock[Transport[Int, Int]]
    when(trans.write(anyInt)).thenReturn(Future.Done)
    when(trans.read()).thenReturn(Future.value(1))
    when(trans.onClose).thenReturn(Future.value(new Exception("boom!")))

    val refTrans = new RefTransport(wrap(trans))

    assert(updateMap(refTrans, _ * 2, _ * 2) == Some(true))

    assert(Await.result(refTrans.read(), 5.seconds) == 2)
    refTrans.write(7)
    verify(trans).write(14)
  }
}
