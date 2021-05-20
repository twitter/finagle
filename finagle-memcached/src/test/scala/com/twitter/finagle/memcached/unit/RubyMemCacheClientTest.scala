package com.twitter.finagle.memcached.unit

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.memcached._
import com.twitter.util.{Await, Future}
import org.mockito.Matchers.any
import org.mockito.Mockito.{times, verify, when}
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class RubyMemCacheClientTest extends AnyFunSuite with MockitoSugar {

  class Context {
    private def newClient(): Client = {
      val c = mock[Client]
      when(c.close(any())).thenReturn(Future.Done)
      c
    }

    val client1 = newClient()
    val client2 = newClient()
    val client3 = newClient()
    val rubyMemCacheClient = new RubyMemCacheClient(Seq(client1, client2, client3))
  }

  test("pick the correct node") {
    val context = new Context
    import context._

    assert(rubyMemCacheClient.clientOf("apple") == (client1))
    assert(rubyMemCacheClient.clientOf("banana") == (client2))
    assert(rubyMemCacheClient.clientOf("cow") == (client1))
    assert(rubyMemCacheClient.clientOf("dog") == (client1))
    assert(rubyMemCacheClient.clientOf("elephant") == (client3))
  }

  test("release") {
    val context = new Context
    import context._

    Await.result(rubyMemCacheClient.close(), 5.seconds)
    verify(client1, times(1)).close(any())
    verify(client2, times(1)).close(any())
    verify(client3, times(1)).close(any())
  }
}
