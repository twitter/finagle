package com.twitter.finagle.mux

import com.twitter.finagle._
import com.twitter.finagle.mux.{transport => mux}
import com.twitter.finagle.netty4.ByteBufAsBuf
import com.twitter.io.Buf
import com.twitter.util.Time
import io.netty.buffer.Unpooled
import io.netty.channel.embedded.EmbeddedChannel
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent.Eventually
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RefCountingTest extends FunSuite with Eventually {

  /**
   * return the total refcount of any direct bytebufs wrapped by `buf`
   */
  def directRefCount(buf: Buf): Int = buf match {
    case bbab: ByteBufAsBuf if bbab.underlying.isDirect => bbab.underlying.refCnt()
    case c: Buf.Composite => c.bufs.foldLeft(0)(_ + directRefCount(_))
    case _ => 0
  }

  val data: Buf = Buf.ByteArray.Owned(Array(1.toByte, 2.toByte, 3.toByte))

  val allMessageTypes: List[Buf] = List(
    mux.Message.Treq(0, None, data),
    mux.Message.RreqOk(0, data),
    mux.Message.RreqError(0, "bad news"),
    mux.Message.RreqNack(0),
    mux.Message.Tdispatch(0, Seq.empty, Path.empty, Dtab.empty, data),
    mux.Message.RdispatchOk(0, Seq.empty, data),
    mux.Message.RdispatchError(0, Seq.empty, "bad"),
    mux.Message.RdispatchNack(0, Seq.empty),
    mux.Message.Tdrain(0),
    mux.Message.Rdrain(0),
    mux.Message.Tping(0),
    mux.Message.Rping(0),
    mux.Message.Tdiscarded(0, "give up already"),
    mux.Message.Rdiscarded(0),
    mux.Message.Tlease(Time.now),
    mux.Message.Tinit(0, 0, Seq.empty),
    mux.Message.Rinit(0, 0, Seq.empty),
    mux.Message.Rerr(0, "bad news")
  ).map(mux.Message.encode)

  test("exceptional decode failures don't leak the underlying buf") {
    val bogus = Buf.U32BE(4).concat(Buf.Utf8("****"))
    val direct = Unpooled.directBuffer(bogus.length)
    direct.writeBytes(Buf.ByteArray.Owned.extract(bogus))

    val e = new EmbeddedChannel()
    mux.RefCountingFramer(e.pipeline())
    e.writeInbound(direct)

    val fail = intercept[Failure] {
      val buf: Buf = e.readInbound[Buf]()
      mux.Message.decode(buf)
    }
    assert(fail.cause.get.isInstanceOf[mux.BadMessageException])

    assert(direct.refCnt == 0)
  }

  test("all messages are released after decode") {
    // we create a single direct byte buffer which encodes all message types so that we
    // test the more interesting decoding path which includes derived + retained buffer
    // slices.
    val bytesWithLenHdr = allMessageTypes.map(msg => Buf.ByteArray.Owned.extract(Buf.U32BE(msg.length).concat(msg)))
    val direct = Unpooled.directBuffer(bytesWithLenHdr.map(_.length).sum)
    val msgOuts = collection.mutable.ListBuffer.empty[mux.Message]
    bytesWithLenHdr.foreach(direct.writeBytes(_))

    val e = new EmbeddedChannel()
    mux.RefCountingFramer(e.pipeline())
    e.writeInbound(direct)

    (1 to allMessageTypes.length).foreach { _ =>
      val framed = e.readInbound[Buf]()
      msgOuts.append(mux.Message.decode(framed))
    }


    msgOuts.foreach { msg => assert(directRefCount(msg.buf) == 0) }
    // after the constituent slices have been decoded the original direct buffer is finally released.
    assert(direct.refCnt() == 0)
  }
}
