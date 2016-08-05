package com.twitter.finagle.http2

import com.twitter.conversions.time._
import com.twitter.finagle.transport.{Transport, TransportProxy}
import com.twitter.util.{Await, Future, Time}
import org.junit.runner.RunWith
import org.mockito.Matchers.{anyInt, any}
import org.mockito.Mockito.{verify, when}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class RefTransportTest extends FunSuite with MockitoSugar {

  // wrapping the transport in a thin proxy provides a correct map
  // implementation
  private[this] def wrap(
    trans: Transport[Int, Int]
  ): Transport[Int, Int] = new TransportProxy(trans) {
    override def write(in: Int): Future[Unit] = trans.write(in)
    override def read(): Future[Int] = trans.read()
  }

  private[this] def updateMap(
    trans: RefTransport[Int, Int],
    left: Int => Int,
    right: Int => Int
  ): Boolean = trans.update(_.map(left, right))

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

    assert(updateMap(refTrans, _ * 2, _ * 3))

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

    assert(updateMap(refTrans, _ * 2, _ * 3))

    assert(Await.result(refTrans.read(), 5.seconds) == 3)

    updateMap(refTrans, _ * 8, _ * 1)
    refTrans.write(7)
    verify(trans).write(56)
  }

  test("RefTransport will no longer update after closing") {
    val trans = mock[Transport[Int, Int]]
    when(trans.write(anyInt)).thenReturn(Future.Done)
    when(trans.read()).thenReturn(Future.value(1))
    when(trans.onClose).thenReturn(Future.never)
    when(trans.close(any[Time])).thenReturn(Future.never)
    val refTrans = new RefTransport(wrap(trans))
    refTrans.close()

    assert(!updateMap(refTrans, _ * 2, _ * 2))

    assert(Await.result(refTrans.read(), 5.seconds) == 1)
    refTrans.write(7)
    verify(trans).write(7)
  }

  test("RefTransport will no longer update after the underlying transport is closed") {
    val trans = mock[Transport[Int, Int]]
    when(trans.write(anyInt)).thenReturn(Future.Done)
    when(trans.read()).thenReturn(Future.value(1))
    when(trans.onClose).thenReturn(Future.value(new Exception("boom!")))

    val refTrans = new RefTransport(wrap(trans))

    assert(!updateMap(refTrans, _ * 2, _ * 2))

    assert(Await.result(refTrans.read(), 5.seconds) == 1)
    refTrans.write(7)
    verify(trans).write(7)
  }
}
