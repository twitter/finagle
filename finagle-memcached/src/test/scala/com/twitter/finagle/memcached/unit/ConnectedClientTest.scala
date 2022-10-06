package com.twitter.finagle.memcached.unit

import com.twitter.conversions.DurationOps._
import com.twitter.io.Buf
import com.twitter.finagle.memcached._
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.Service
import com.twitter.util.Await
import com.twitter.util.Awaitable
import com.twitter.util.Future
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class ConnectedClientTest extends AnyFunSuite with MockitoSugar {

  val TimeOut = 15.seconds

  private def awaitResult[T](awaitable: Awaitable[T]): T = Await.result(awaitable, TimeOut)

  val service = mock[Service[Command, Response]]
  val client = Client.apply(service)
  val casUnique = Buf.Utf8("unique key")
  val key = "key"
  val value = Buf.Utf8("value")

  test("cas correctly responds to return states of the service") {
    when(service.apply(any[Command])).thenReturn(Future.value(Stored))
    assert(awaitResult(client.checkAndSet(key, value, casUnique).map(_.replaced)))

    when(service.apply(any[Command])).thenReturn(Future.value(Exists))
    assert(!awaitResult(client.checkAndSet(key, value, casUnique).map(_.replaced)))

    when(service.apply(any[Command])).thenReturn(Future.value(NotFound))
    assert(!awaitResult(client.checkAndSet(key, value, casUnique).map(_.replaced)))
  }

  test("checkAndSet correctly responds to return states of the service") {
    when(service.apply(any[Command])).thenReturn(Future.value(Stored))
    assert(awaitResult(client.checkAndSet(key, value, casUnique)) == CasResult.Stored)

    when(service.apply(any[Command])).thenReturn(Future.value(Exists))
    assert(awaitResult(client.checkAndSet(key, value, casUnique)) == CasResult.Exists)

    when(service.apply(any[Command])).thenReturn(Future.value(NotFound))
    assert(awaitResult(client.checkAndSet(key, value, casUnique)) == CasResult.NotFound)
  }

  test("checkAndSet correctly responds to the error states of the service") {
    when(service.apply(any[Command]))
      .thenReturn(Future.value(Error(new IllegalAccessException("exception"))))
    intercept[IllegalAccessException] { awaitResult(client.checkAndSet(key, value, casUnique)) }

    when(service.apply(any[Command])).thenReturn(Future.value(Deleted))
    intercept[IllegalStateException] { awaitResult(client.checkAndSet(key, value, casUnique)) }
  }
}
