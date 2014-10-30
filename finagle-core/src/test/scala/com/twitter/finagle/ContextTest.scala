package com.twitter.finagle

import com.twitter.io.Buf
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}

class MyContext extends ContextHandler {
  @volatile var allBuf = Buf.Empty

  val key = Buf.Utf8("com.twitter.finagle.MyContext")

  def handle(buf: Buf) {
    synchronized {
      allBuf = allBuf concat buf
    }
  }

  def emit() = Some(allBuf)
}

@RunWith(classOf[JUnitRunner])
class ContextTest extends FunSuite with AssertionsForJUnit {
  def contextMap = Context.emit().map { case (Buf.Utf8(k), v) => k -> v }.toMap

  def assertPassthru(key: String) {
    val key8 = Buf.Utf8(key)

    Context.handle(key8, Buf.ByteArray(1,2,3,4))
    contextMap.get(key) match {
      case Some(buf) =>
        assert(buf === Buf.ByteArray(1,2,3,4))
      case None =>
        fail("Map doesn't contain key " + key)
    }

    Context.handle(key8, Buf.ByteArray(5,6,7,8))
    assert(contextMap(key) === Buf.ByteArray(5,6,7,8))
  }

  test("resolve by class, instantiating once for each key") {
    val K = "com.twitter.finagle.MyContext"
    val K8 = Buf.Utf8(K)

    Context.handle(K8, Buf.ByteArray(1,2,3,4))
    contextMap.get(K) match {
      case Some(buf) =>
        assert(buf === Buf.ByteArray(1,2,3,4))
      case None =>
        fail("Map doesn't contain K " + K)
    }

    Context.handle(K8, Buf.ByteArray(5,6,7,8))
    assert(contextMap(K) === Buf.ByteArray(1,2,3,4,5,6,7,8))
  }

  test("pass through unknown contexts") {
    assertPassthru("com.twitter.finagle.Nope")
  }
}
