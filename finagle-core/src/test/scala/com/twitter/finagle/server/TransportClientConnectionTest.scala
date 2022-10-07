package com.twitter.finagle.server

import com.twitter.finagle.ssl.session.NullSslSessionInfo
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.transport.TransportContext
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Promise
import com.twitter.util.Return
import com.twitter.util.Time
import java.net.SocketAddress
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.stubbing.OngoingStubbing
import org.scalatestplus.mockito.MockitoSugar
import scala.language.reflectiveCalls
import org.scalatest.funsuite.AnyFunSuite

class TransportClientConnectionTest extends AnyFunSuite with MockitoSugar {

  // Don't let the Scala compiler get confused about which `thenReturn`
  // method we want to use.
  private[this] def when[T](o: T) =
    Mockito.when(o).asInstanceOf[{ def thenReturn[RT](s: RT): OngoingStubbing[RT] }]

  test("remoteAddress returns the TransportContext's remoteAddress") {
    val trans = mock[Transport[Unit, Unit]]
    val context = mock[TransportContext]
    val remoteAddress = new SocketAddress {}
    when(trans.context).thenReturn(context)
    when(trans.onClose).thenReturn(Future.never)
    when(context.remoteAddress).thenReturn(remoteAddress)

    val conn = new TransportClientConnection(trans)
    assert(conn.remoteAddress == remoteAddress)
  }

  test("localAddress returns the TransportContext's localAddress") {
    val trans = mock[Transport[Unit, Unit]]
    val context = mock[TransportContext]
    val localAddress = new SocketAddress {}
    when(trans.context).thenReturn(context)
    when(trans.onClose).thenReturn(Future.never)
    when(context.localAddress).thenReturn(localAddress)

    val conn = new TransportClientConnection(trans)
    assert(conn.localAddress == localAddress)
  }

  test("onClose returns the Transport's onClose as unit") {
    val trans = mock[Transport[Unit, Unit]]
    val promise = new Promise[Int]
    when(trans.onClose).thenReturn(promise)

    val conn = new TransportClientConnection(trans)
    val result = conn.onClose
    // asserts are written out this way because using just
    // `isDone` results in the compiler warning mentioned in
    // https://github.com/scalatest/scalatest/issues/961
    assert(result.isDefined == false)
    promise.updateIfEmpty(Return(5))
    assert(result.isDefined == true)
  }

  test("close calls the Transport's close method") {
    val trans = mock[Transport[Unit, Unit]]
    val context = mock[TransportContext]
    when(trans.context).thenReturn(context)
    when(trans.onClose).thenReturn(Future.never)

    val conn = new TransportClientConnection(trans)
    verify(trans, times(0)).close(any[Time])
    conn.close(Duration.fromSeconds(1))
    verify(trans, times(1)).close(any[Time])
  }

  test("sslSessionInfo returns the TransportContext's sslSessionInfo") {
    val trans = mock[Transport[Unit, Unit]]
    val context = mock[TransportContext]
    when(trans.context).thenReturn(context)
    when(trans.onClose).thenReturn(Future.never)
    when(context.sslSessionInfo).thenReturn(NullSslSessionInfo)

    val conn = new TransportClientConnection(trans)
    assert(conn.sslSessionInfo == NullSslSessionInfo)
  }
}
