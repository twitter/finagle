package com.twitter.finagle.netty4

import com.twitter.io.Buf
import io.netty.buffer.Unpooled
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BufsTest extends FunSuite {

  test("BufUtil#release releases wrapped ref-counted n4 buffers") {
    val n4buf = Unpooled.directBuffer(1)
    n4buf.writeByte(1)
    assert(n4buf.refCnt() == 1)
    val wrapped = ByteBufAsBuf.Owned(n4buf)
    Bufs.releaseDirect(wrapped)
    assert(n4buf.refCnt() == 0)
  }


  test("BufUtil#release recursively releases concat buf contents") {
    val n4bufs =
      List(
        Unpooled.directBuffer(1),
        Unpooled.directBuffer(1),
        Unpooled.directBuffer(1)
      )

    n4bufs(0).writeByte(1)
    n4bufs(1).writeByte(2)
    n4bufs(2).writeByte(3)

    assert(n4bufs.forall(_.refCnt() == 1))
    val wrapped = Buf(n4bufs.map(ByteBufAsBuf.Owned(_)))

    Bufs.releaseDirect(wrapped)
    assert(n4bufs.forall(_.refCnt() == 0))
  }

  test("BufUtil#copyAndReleaseDirect copies and releases direct buffers") {
    val n4buf = Unpooled.directBuffer(3)
    n4buf.writeByte(1)
    n4buf.writeByte(2)
    n4buf.writeByte(3)
    assert(n4buf.refCnt() == 1)
    val wrapped = ByteBufAsBuf.Owned(n4buf)
    val res: Buf = Bufs.copyAndReleaseDirect(wrapped)
    assert(n4buf.refCnt() == 0)
    assert(Buf.slowHexString(res) == "010203")
  }

  test("BufUtil#copyAndReleaseDirect copies and releases direct buffers in concat bufs") {
    val n4bufs =
      List(
        Unpooled.directBuffer(1),
        Unpooled.directBuffer(1),
        Unpooled.directBuffer(1)
      )

    assert(n4bufs.forall(_.refCnt() == 1))
    n4bufs(0).writeByte(1)
    n4bufs(1).writeByte(2)
    n4bufs(2).writeByte(3)
    val wrapped = Buf(n4bufs.map(ByteBufAsBuf.Owned(_)))
    val res: Buf = Bufs.copyAndReleaseDirect(wrapped)
    assert(n4bufs.forall(_.refCnt() == 0))

    assert(Buf.slowHexString(res) == "010203")
  }
}
