package com.twitter.finagle.memcached.unit

import com.twitter.finagle.memcached._
import org.junit.runner.RunWith
import org.mockito.Mockito.{times, verify}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class RubyMemCacheClientTest extends FunSuite with MockitoSugar {

  class Context {
    val client1 = mock[Client]
    val client2 = mock[Client]
    val client3 = mock[Client]
    val rubyMemCacheClient = new RubyMemCacheClient(Seq(client1, client2, client3))
  }

  test("pick the correct node") {
    val context = new Context
    import context._

    assert(rubyMemCacheClient.clientOf("apple")    == (client1))
    assert(rubyMemCacheClient.clientOf("banana")   == (client2))
    assert(rubyMemCacheClient.clientOf("cow")      == (client1))
    assert(rubyMemCacheClient.clientOf("dog")      == (client1))
    assert(rubyMemCacheClient.clientOf("elephant") == (client3))
  }

  test("release") {
    val context = new Context
    import context._

    rubyMemCacheClient.release()
    verify(client1, times(1)).release()
    verify(client2, times(1)).release()
    verify(client3, times(1)).release()
  }
}
