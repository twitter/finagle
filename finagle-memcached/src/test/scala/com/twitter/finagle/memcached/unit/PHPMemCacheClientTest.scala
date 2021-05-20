package com.twitter.finagle.memcached.unit

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.memcached._
import com.twitter.hashing.KeyHasher
import com.twitter.util.{Await, Future}
import org.mockito.Matchers.any
import org.mockito.Mockito.{verify, when}
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class PHPMemCacheClientTest extends AnyFunSuite with MockitoSugar {

  class Context {
    private def newClient(): Client = {
      val c = mock[Client]
      when(c.close(any())).thenReturn(Future.Done)
      c
    }

    val client1 = newClient()
    val client2 = newClient()
    val client3 = newClient()
    val phpMemCacheClient =
      new PHPMemCacheClient(Array(client1, client2, client3), KeyHasher.FNV1_32)
  }

  test("pick the correct node") {
    val context = new Context
    import context._

    assert(phpMemCacheClient.clientOf("apple") == (client3))
    assert(phpMemCacheClient.clientOf("banana") == (client1))
    assert(phpMemCacheClient.clientOf("cow") == (client3))
    assert(phpMemCacheClient.clientOf("dog") == (client2))
    assert(phpMemCacheClient.clientOf("elephant") == (client2))
  }

  test("release") {
    val context = new Context
    import context._

    Await.result(phpMemCacheClient.close(), 5.seconds)
    verify(client1).close(any())
    verify(client2).close(any())
    verify(client3).close(any())
  }
}
