package com.twitter.finagle.mux.exp.pushsession

import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.{Dtab, Path}
import com.twitter.finagle.mux.transport.Message.{Fragment, Tags, Tdiscarded, Tdispatch}
import com.twitter.finagle.netty4.CopyingByteBufByteReader
import com.twitter.io.{Buf, ByteReader}
import io.netty.buffer.ByteBufAllocator
import org.scalatest.FunSuite

class FragmentDecoderTest extends FunSuite {

  test("decodes whole message") {
    val decoder = new FragmentDecoder
    val msg = Tdispatch(2, Seq.empty, Path(), Dtab.empty, Buf.ByteArray(1, 2, 3))
    val data = ByteReader(Message.encode(msg))
    assert(decoder.decode(data) == msg)
  }

  test("decodes a fragment") {
    val decoder = new FragmentDecoder
    val tag = 2
    val msg = Tdispatch(tag, Seq.empty, Path(), Dtab.empty, Buf.ByteArray(1, 2, 3, 4))
    val buf = Message.encode(msg)
    val body = buf.slice(4, Int.MaxValue)

    // Not a full message
    val frag1 = Fragment(msg.typ, Tags.setMsb(tag), body.slice(0, 4))
    assert(decoder.decode(ByteReader(Message.encode(frag1))) == null)
    // final fragment
    val frag2 = buf.slice(0, 4).concat(body.slice(4, Int.MaxValue))
    assert(decoder.decode(ByteReader(frag2)) == msg)
  }

  test("discards fragments in the event of a Tdiscard for that tag") {
    val decoder = new FragmentDecoder
    val tag = 2
    val msg = Tdispatch(tag, Seq.empty, Path(), Dtab.empty, Buf.ByteArray(1, 2, 3, 4))
    val buf = Message.encode(msg)
    val body = buf.slice(4, Int.MaxValue)

    // Not a full message
    val frag1 = Fragment(msg.typ, Tags.setMsb(tag), body.slice(0, 4))
    assert(decoder.decode(ByteReader(Message.encode(frag1))) == null)
    // Tdiscard
    decoder.decode(ByteReader(Message.encode(Tdiscarded(tag, "don't care anymore"))))
    // Full message on same tag shouldn't be corrupted by the Tdiscard
    assert(decoder.decode(ByteReader(buf)) == msg)
  }

  test("releases the ByteReader") {
    val decoder = new FragmentDecoder
    val msg = Tdispatch(2, Seq.empty, Path(), Dtab.empty, Buf.ByteArray(1, 2, 3))
    val buf = Message.encode(msg)
    val bb = ByteBufAllocator.DEFAULT.buffer(buf.length)
    bb.writeBytes(Buf.ByteArray.Owned.extract(buf))

    val br = new CopyingByteBufByteReader(bb)
    assert(bb.refCnt() == 1)
    assert(decoder.decode(br) == msg)
    assert(bb.refCnt() == 0)
  }
}
