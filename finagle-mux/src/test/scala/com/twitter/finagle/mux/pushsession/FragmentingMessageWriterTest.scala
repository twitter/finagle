package com.twitter.finagle.mux.pushsession

import com.twitter.finagle.pushsession.utils.MockChannelHandle
import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.{Dtab, Path}
import com.twitter.finagle.mux.transport.Message.Tdispatch
import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver}
import com.twitter.io.Buf
import com.twitter.util.Return
import org.scalatest.funsuite.AnyFunSuite

class FragmentingMessageWriterTest extends AnyFunSuite {

  private class PendingStreamStatsReceiver extends InMemoryStatsReceiver {
    def pendingStreamCount: Int = gauges(Seq("mux", "framer", "pending_write_streams"))().toInt
    def writes: Seq[Int] = stats(Seq("mux", "framer", "write_stream_bytes")).map(_.toInt)
  }

  private val Tag = 2
  private val Data = Buf.ByteArray((0 until 1024).map(_.toByte): _*)

  private def concat(msgs: Iterable[Buf]): Buf = msgs.foldLeft(Buf.Empty)(_.concat(_))

  test("writes that don't overflow the max frame size are not fragmented") {
    val sr = new PendingStreamStatsReceiver
    val handle = new MockChannelHandle[Any, Buf]()
    val msg = Tdispatch(Tag, Seq.empty, Path(), Dtab.empty, Data)
    val writer = new FragmentingMessageWriter(handle, msg.buf.length, sr)
    writer.write(msg)
    assert(Message.decode(concat(handle.pendingWrites.dequeue().msgs)) == msg)
    assert(sr.pendingStreamCount == 0)
  }

  test("writes that do overflow the max frame size are fragmented") {
    val sr = new PendingStreamStatsReceiver
    val handle = new MockChannelHandle[Any, Buf]()
    val msg = Tdispatch(Tag, Seq.empty, Path(), Dtab.empty, Data)
    val writer = new FragmentingMessageWriter(handle, msg.buf.length - 1, sr)
    writer.write(msg)
    assert(handle.pendingWrites.size == 1)
    assert(sr.pendingStreamCount == 1)
    assert(sr.writes == Seq(msg.buf.length - 1 + 4))

    val handle.SendOne(data1, thunk) = handle.pendingWrites.dequeue()
    thunk(Return.Unit)

    assert(handle.pendingWrites.size == 1)
    assert(sr.pendingStreamCount == 0)
    assert(sr.writes == Seq(msg.buf.length - 1 + 4, 1 + 4))

    val handle.SendOne(data2, _) = handle.pendingWrites.dequeue()

    val fullMessage =
      data2.slice(0, 4).concat(data1.slice(4, Int.MaxValue)).concat(data2.slice(4, Int.MaxValue))

    assert(Message.decode(fullMessage) == msg)
  }

  test("write interests can be discarded") {
    val sr = new PendingStreamStatsReceiver

    val handle = new MockChannelHandle[Any, Buf]()
    val msg = Tdispatch(Tag, Seq.empty, Path(), Dtab.empty, Data)
    val writer = new FragmentingMessageWriter(handle, msg.buf.length - 1, sr)
    writer.write(msg)
    assert(sr.pendingStreamCount == 1)

    assert(writer.removeForTag(Tag) == MessageWriter.DiscardResult.PartialWrite)
    assert(sr.pendingStreamCount == 0)
  }

  test("we don't remove interests for non-matching tags") {
    val sr = new PendingStreamStatsReceiver
    val handle = new MockChannelHandle[Any, Buf]()
    val msg = Tdispatch(Tag, Seq.empty, Path(), Dtab.empty, Data)
    val writer = new FragmentingMessageWriter(handle, msg.buf.length - 1, sr)
    writer.write(msg)
    assert(sr.pendingStreamCount == 1)

    assert(writer.removeForTag(Tag + 1) == MessageWriter.DiscardResult.NotFound)
    assert(sr.pendingStreamCount == 1)
  }

  test("Removes interests that have not had any fragments written") {
    val sr = new PendingStreamStatsReceiver
    val handle = new MockChannelHandle[Any, Buf]()
    val msg1 = Tdispatch(Tag, Seq.empty, Path(), Dtab.empty, Data)
    val msg2 = Tdispatch(Tag + 1, Seq.empty, Path(), Dtab.empty, Data)
    val writer = new FragmentingMessageWriter(handle, msg1.buf.length - 1, sr)
    writer.write(msg1)
    assert(sr.pendingStreamCount == 1)

    writer.write(msg2)
    assert(sr.pendingStreamCount == 2)

    assert(writer.removeForTag(Tag + 1) == MessageWriter.DiscardResult.Unwritten)
    assert(sr.pendingStreamCount == 1)
  }

  test("drain() notifies after writes finish") {
    val handle = new MockChannelHandle[Any, Buf]()
    val writer = new FragmentingMessageWriter(handle, Int.MaxValue, NullStatsReceiver)

    // We reuse the same message, even though its illegal per the mux spec, for convenience
    val msg = Tdispatch(Tag, Seq.empty, Path(), Dtab.empty, Data)

    // write one message
    writer.write(msg)
    val drainP = writer.drain()
    assert(!drainP.isDefined)
    handle.dequeAndCompleteWrite() // Don't care about the data
    assert(drainP.isDefined) // should be done now
  }

  test("drain() will allow further writes if we're draining") {
    val handle = new MockChannelHandle[Any, Buf]()
    val writer = new FragmentingMessageWriter(handle, Int.MaxValue, NullStatsReceiver)

    // We reuse the same message, even though its illegal per the mux spec, for convenience
    val msg = Tdispatch(Tag, Seq.empty, Path(), Dtab.empty, Data)

    // write one message
    writer.write(msg)
    val drainP = writer.drain()
    assert(!drainP.isDefined)

    // Now two messages
    writer.write(msg)

    handle.dequeAndCompleteWrite() // Don't care about the data
    assert(!drainP.isDefined) // Still a pending message

    handle.dequeAndCompleteWrite() // Don't care about the data
    assert(drainP.isDefined) // Should be done now
  }

  test("drain() notifies immediately if nothing is pending") {
    val handle = new MockChannelHandle[Any, Buf]()
    val writer = new FragmentingMessageWriter(handle, Int.MaxValue, NullStatsReceiver)

    // Already idle
    assert(writer.drain().isDefined)
  }
}
