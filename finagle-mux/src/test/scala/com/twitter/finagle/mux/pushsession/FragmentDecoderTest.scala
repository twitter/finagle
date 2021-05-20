package com.twitter.finagle.mux.pushsession

import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.{Dtab, Path}
import com.twitter.finagle.mux.transport.Message.{Fragment, Tags, Tdiscarded, Tdispatch}
import com.twitter.finagle.netty4.CopyingByteBufByteReader
import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver}
import com.twitter.io.{Buf, ByteReader}
import io.netty.buffer.ByteBufAllocator
import org.scalatest.funsuite.AnyFunSuite

class FragmentDecoderTest extends AnyFunSuite {

  private class DecoderStatsReceiver extends InMemoryStatsReceiver {
    def pendingReadStreams: Int = gauges(Seq("mux", "framer", "pending_read_streams"))().toInt
    def readBytes: Seq[Int] = stats(Seq("mux", "framer", "read_stream_bytes")).map(_.toInt)
  }

  test("decodes whole message") {
    val sr = new DecoderStatsReceiver
    val decoder = new FragmentDecoder(sr)
    val msg = Tdispatch(2, Seq.empty, Path(), Dtab.empty, Buf.ByteArray(1, 2, 3))
    val data = ByteReader(Message.encode(msg))
    val dataSize = data.remaining
    assert(decoder.decode(data) == msg)
    assert(sr.pendingReadStreams == 0)
    assert(sr.readBytes == Seq(dataSize))
  }

  test("decodes a fragment") {
    val sr = new DecoderStatsReceiver
    val decoder = new FragmentDecoder(sr)
    val tag = 2
    val msg = Tdispatch(tag, Seq.empty, Path(), Dtab.empty, Buf.ByteArray(1, 2, 3, 4))
    val buf = Message.encode(msg)
    val body = buf.slice(4, Int.MaxValue)

    // Not a full message
    val msgBuf1 = Message.encode(Fragment(msg.typ, Tags.setMsb(tag), body.slice(0, 4)))
    assert(decoder.decode(ByteReader(msgBuf1)) == null)
    assert(sr.pendingReadStreams == 1)
    assert(sr.readBytes == Seq(msgBuf1.length))

    // final fragment
    val msgBuf2 = buf.slice(0, 4).concat(body.slice(4, Int.MaxValue))
    assert(decoder.decode(ByteReader(msgBuf2)) == msg)
    assert(sr.pendingReadStreams == 0)
    assert(sr.readBytes == Seq(msgBuf1.length, msgBuf2.length))
  }

  test("discards fragments in the event of a Tdiscard for that tag") {
    val sr = new DecoderStatsReceiver
    val decoder = new FragmentDecoder(sr)
    val tag = 2
    val msg = Tdispatch(tag, Seq.empty, Path(), Dtab.empty, Buf.ByteArray(1, 2, 3, 4))
    val buf = Message.encode(msg)
    val body = buf.slice(4, Int.MaxValue)

    // Not a full message
    val msgBuf1 = Message.encode(Fragment(msg.typ, Tags.setMsb(tag), body.slice(0, 4)))
    assert(decoder.decode(ByteReader(msgBuf1)) == null)
    assert(sr.pendingReadStreams == 1)
    assert(sr.readBytes == Seq(msgBuf1.length))
    // Tdiscard
    val tdiscarded = Tdiscarded(tag, "don't care anymore")
    val msgBuf2 = Message.encode(tdiscarded)
    assert(decoder.decode(ByteReader(msgBuf2)) == tdiscarded)
    assert(sr.pendingReadStreams == 0)
    assert(sr.readBytes == Seq(msgBuf1.length, msgBuf2.length))
    // Full message on same tag shouldn't be corrupted by the Tdiscard
    assert(decoder.decode(ByteReader(buf)) == msg)
  }

  test("releases the ByteReader") {
    val decoder = new FragmentDecoder(NullStatsReceiver)
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
