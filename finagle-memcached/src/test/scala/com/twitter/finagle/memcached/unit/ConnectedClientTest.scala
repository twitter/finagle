package com.twitter.finagle.memcached.unit

import com.twitter.io.Buf
import com.twitter.finagle.memcached._
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.Service
import com.twitter.util.{ Await, Future }
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class ConnectedClientTest extends FunSuite with MockitoSugar {

  val service = mock[Service[Command, Response]]
  val client = Client.apply(service)
  val casUnique = Buf.Utf8("unique key")
  val key = "key"
  val value = Buf.Utf8("value")

  test("cas correctly responds to return states of the service") {
    when(service.apply(any[Command])).thenReturn(Future.value(Stored()))
    assert(Await.result(client.cas(key, value, casUnique)) == true)

    when(service.apply(any[Command])).thenReturn(Future.value(Exists()))
    assert(Await.result(client.cas(key, value, casUnique)) == false)

    when(service.apply(any[Command])).thenReturn(Future.value(NotFound()))
    assert(Await.result(client.cas(key, value, casUnique)) == false)
 }

  test("cas correctly responds to the error states of the service") {
    when(service.apply(any[Command])).thenReturn(Future.value(Error(new IllegalAccessException("exception"))))
    intercept[IllegalAccessException] { Await.result(client.cas(key, value, casUnique)) }

    when(service.apply(any[Command])).thenReturn(Future.value(Deleted()))
    intercept[IllegalStateException] { Await.result(client.cas(key, value, casUnique)) }
  }
}

