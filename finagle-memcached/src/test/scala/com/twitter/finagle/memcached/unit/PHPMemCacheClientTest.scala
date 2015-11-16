package com.twitter.finagle.memcached.unit

import com.twitter.finagle.memcached._
import com.twitter.hashing.KeyHasher
import org.junit.runner.RunWith
import org.mockito.Mockito.verify
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class PHPMemCacheClientTest extends FunSuite with MockitoSugar {

  class Context {
    val client1 = mock[Client]
    val client2 = mock[Client]
    val client3 = mock[Client]
    val phpMemCacheClient = new PHPMemCacheClient(Array(client1, client2, client3), KeyHasher.FNV1_32)
  }

  test("pick the correct node") {
    val context = new Context
    import context._

    assert(phpMemCacheClient.clientOf("apple")    == (client3))
    assert(phpMemCacheClient.clientOf("banana")   == (client1))
    assert(phpMemCacheClient.clientOf("cow")      == (client3))
    assert(phpMemCacheClient.clientOf("dog")      == (client2))
    assert(phpMemCacheClient.clientOf("elephant") == (client2))
  }

  test("release") {
    val context = new Context
    import context._

    phpMemCacheClient.release()
    verify(client1).release()
    verify(client2).release()
    verify(client3).release()
  }
}
