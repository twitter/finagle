package com.twitter.finagle.mux.exp.pushsession

import com.twitter.finagle.exp.pushsession.MockChannelHandle
import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.{Dtab, Path}
import com.twitter.finagle.mux.transport.Message.Tdispatch
import com.twitter.io.Buf
import com.twitter.util.Return
import org.scalatest.FunSuite

class FragmentingMessageWriterTest extends FunSuite {
  private val Tag = 2
  private val Data = Buf.ByteArray((0 until 1024).map(_.toByte):_*)

  private def concat(msgs: Iterable[Buf]): Buf = msgs.foldLeft(Buf.Empty)(_.concat(_))

  test("writes that don't overflow the max frame size are not fragmented") {
    val handle = new MockChannelHandle[Any, Buf]()
    val msg = Tdispatch(Tag, Seq.empty, Path(), Dtab.empty, Data)
    val writer = new FragmentingMessageWriter(handle, msg.buf.length)
    writer.write(msg)
    assert(Message.decode(concat(handle.pendingWrites.dequeue().msgs)) == msg)
    assert(writer.messageQueue.isEmpty)
  }

  test("writes that do overflow the max frame size are fragmented") {
    val handle = new MockChannelHandle[Any, Buf]()
    val msg = Tdispatch(Tag, Seq.empty, Path(), Dtab.empty, Data)
    val writer = new FragmentingMessageWriter(handle, msg.buf.length - 1)
    writer.write(msg)
    assert(handle.pendingWrites.size == 1)
    assert(writer.messageQueue.size == 1)
    val handle.SendOne(data1, thunk) = handle.pendingWrites.dequeue()
    thunk(Return.Unit)

    assert(handle.pendingWrites.size == 1)
    assert(writer.messageQueue.isEmpty)
    val handle.SendOne(data2, _) = handle.pendingWrites.dequeue()

    val fullMessage = data2.slice(0, 4).concat(data1.slice(4, Int.MaxValue)).concat(data2.slice(4, Int.MaxValue))

    assert(Message.decode(fullMessage) == msg)
    assert(writer.messageQueue.isEmpty)
  }

  test("write interests can be discarded") {
    val handle = new MockChannelHandle[Any, Buf]()
    val msg = Tdispatch(Tag, Seq.empty, Path(), Dtab.empty, Data)
    val writer = new FragmentingMessageWriter(handle, msg.buf.length - 1)
    writer.write(msg)
    assert(writer.messageQueue.size == 1)
    assert(writer.removeForTag(Tag) == MessageWriter.DiscardResult.PartialWrite)
    assert(writer.messageQueue.isEmpty)
  }

  test("we don't remove interests for non-matching tags") {
    val handle = new MockChannelHandle[Any, Buf]()
    val msg = Tdispatch(Tag, Seq.empty, Path(), Dtab.empty, Data)
    val writer = new FragmentingMessageWriter(handle, msg.buf.length - 1)
    writer.write(msg)
    assert(writer.messageQueue.size == 1)
    assert(writer.removeForTag(Tag + 1) == MessageWriter.DiscardResult.NotFound)
    assert(writer.messageQueue.size == 1)
  }

  test("Removes interests that have not had any fragments written") {
    val handle = new MockChannelHandle[Any, Buf]()
    val msg1 = Tdispatch(Tag, Seq.empty, Path(), Dtab.empty, Data)
    val msg2 = Tdispatch(Tag + 1, Seq.empty, Path(), Dtab.empty, Data)
    val writer = new FragmentingMessageWriter(handle, msg1.buf.length - 1)
    writer.write(msg1)
    assert(writer.messageQueue.size == 1)
    writer.write(msg2)
    assert(writer.messageQueue.size == 2)
    assert(writer.removeForTag(Tag + 1) == MessageWriter.DiscardResult.Unwritten)
    assert(writer.messageQueue.size == 1)
  }
}
