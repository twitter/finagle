package com.twitter.finagle.mux.transport

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.time._
import com.twitter.finagle.transport.{QueueTransport, TransportProxy}
import com.twitter.finagle.{Dtab, Dentry, Failure, Path}
import com.twitter.util.{Await, Future, Promise, Return}
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers
import org.junit.runner.RunWith
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.GeneratorDrivenPropertyChecks

@RunWith(classOf[JUnitRunner])
class MuxFramerTest extends FunSuite with GeneratorDrivenPropertyChecks {
  import MuxFramerTest._

  test("writes") (forAll { (msg: Message, window: Int) =>
    whenever(window > 0) {
      val writeq = new AsyncQueue[ChannelBuffer]
      val readq = new AsyncQueue[ChannelBuffer]
      // Create a queued transport that seals writes as if we've written
      // the bytes to the wire. Note, this is important since the
      // written channel buffers share buffer regions to avoid allocations.
      val transport = new QueueTransport(writeq, readq)
        .map({ buf: ChannelBuffer => buf.copy() }, identity)

      val flow = MuxFramer(transport, window)
      // note, this relies on the flush timer
      Await.result(flow.write(msg), 5.seconds)

      val payloadSize = msg.buf.readableBytes

      if (window >= payloadSize) assert(writeq.size == 1) else {
        val Return(q) = writeq.drain()

        // ensure that we've written the correct amount of bytes
        val written = q.foldLeft(0) { _ + _.readableBytes }
        val expected = Message.encode(msg).readableBytes
        val hdrSize = 4 * (q.size-1)
        assert(written == expected + hdrSize)

        q.init.foreach { buf =>
          buf.readByte() // typ
          assert((buf.readByte() >> 7 & 1) == 1) // tag MSB
        }

        val last = q.last
        last.readByte() // typ
        assert((last.readByte() >> 7 & 1) == 0) // tag MSB
      }
    }
  })

  test("reads") (forAll { (msg: Message, window: Int) =>
    whenever(window > 0) {
      val transq = new AsyncQueue[ChannelBuffer]
      val transport = new QueueTransport(transq, transq)
        .map({ buf: ChannelBuffer => buf.copy() }, identity)

      val flow = MuxFramer(transport, window)
      // note, this relies on the flush timer
      Await.result(flow.write(msg), 5.seconds)

      def go(): Future[Message] =
        flow.read().flatMap { m => if (m == msg) Future.value(m) else go() }

      val out = Await.result(go(), 5.seconds)
      val expected = ChannelBuffers.hexDump(Message.encode(msg))
      val received = ChannelBuffers.hexDump(Message.encode(out))
      assert(received == expected)
    }
  })

  test("multiple round-trip") (forAll { messages: Seq[Message] =>
    // make sure messages have unique tags.
    val msgs = messages.foldLeft(Map.empty[Int, Message]) {
      case (map, msg) => map + (msg.tag -> msg)
    }
    whenever(msgs.nonEmpty) {
      val transq = new AsyncQueue[ChannelBuffer]
      val transport = new QueueTransport(transq, transq)
        .map({ buf: ChannelBuffer => buf.copy() }, identity)

      val flow = MuxFramer(transport, 1)
      // note, this relies on the flush timer.
      Await.result(Future.collect(msgs.values.toSeq.map(flow.write)), 5.seconds)

      def go(ms: Map[Int, Message]): Future[Map[Int, Message]] =
        flow.read().flatMap { m =>
          val mms = if (msgs.contains(m.tag)) ms + (m.tag -> m) else ms
          if (mms.size == msgs.size) Future.value(mms) else go(mms)
        }

      assert(msgs == Await.result(go(Map.empty), 5.seconds))
    }
  })

  test("Tdiscarded") {
    val msg = Message.Tdispatch(20, Seq.empty, Path.read("/foo/bar/baz"),
      Dtab.empty, ChannelBuffers.wrappedBuffer(("abcd" * 100).getBytes))

    val writeq = new AsyncQueue[ChannelBuffer]
    val readq = new AsyncQueue[ChannelBuffer]
    val transport = new QueueTransport(writeq, readq)
      .map({ buf: ChannelBuffer => buf.copy() }, identity)

    val flow = MuxFramer(transport, 1)
    // disable the flush timer, we want to manually prime
    flow.close()

    val write = flow.write(msg)

    // interrupt
    Await.result(flow.write(Message.Tdiscarded(20, "timeout!")), 5.seconds)
    val failure = intercept[Failure] { Await.result(write, 5.seconds) }
    assert(failure.getMessage == "timeout!")

    val Return(writes) = writeq.drain()
    assert(writes.exists { buf =>
      val typ = Message.Tags.extractType(buf.readInt())
      typ == Message.Types.BAD_Tdiscarded
    })
  }

  test("Rdiscarded") {
    val msg = Message.RdispatchOk(20, Seq.empty,
      ChannelBuffers.wrappedBuffer("abcd".getBytes))

    val transq = new AsyncQueue[ChannelBuffer]
    val writep = new Promise[Unit]

    val underlying = new QueueTransport(transq, transq)
      .map({ buf: ChannelBuffer => buf.copy() }, identity)

    val transport = new TransportProxy(underlying) {
      def write(in: ChannelBuffer) = {
        underlying.write(in)
        writep
      }

      def read = underlying.read
    }

    val flow = MuxFramer(transport, 1)
    // disable the flush timer, we want to manually prime
    flow.close()
    val primer = Message.encode(Message.Tping(10))

    val write = flow.write(msg)
    transq.offer(primer)
    val exc = new Exception("timeout!")
    write.raise(exc)
    writep.setDone()
    transq.offer(primer)
    assert(exc == intercept[Exception] { Await.result(write, 1.second) })

    val Return(writes) = transq.drain()
    assert(writes.exists { buf =>
      val typ = Message.Tags.extractType(buf.readInt())
      typ == Message.Types.Rdiscarded
    })
  }

  test("header endec") (forAll(Gen.choose(1, Int.MaxValue)) { size: Int =>
    assert(MuxFramer.Header.decodeFrameSize(
      MuxFramer.Header.encodeFrameSize(size)) == size)
  })
}

object MuxFramerTest {
  val genMessage: Gen[Message] = for {
    typ <- Gen.oneOf(Seq(Message.Types.Tdispatch, Message.Types.Rdispatch))
    tag <- Gen.choose(Message.Tags.MinTag, Message.Tags.MaxTag)
    ctx <- Gen.alphaStr
    path <- Gen.oneOf(Seq("/", "/okay", "/foo/bar/baz"))
    dentry <- Gen.oneOf(Seq("/a=>/b", "/foo=>/$/inet/twitter.com/80"))
    body <- Gen.alphaStr
  } yield {
    val ctxBuf = ChannelBuffers.wrappedBuffer(ctx.getBytes)
    val contexts = Seq(ctxBuf -> ctxBuf)
    val bodyBuf = ChannelBuffers.wrappedBuffer(body.getBytes)
    if (typ == Message.Types.Tdispatch)
      Message.Tdispatch(tag, contexts,
        Path.read(path), Dtab(IndexedSeq(Dentry.read(dentry))), bodyBuf)
    else
      Message.RdispatchOk(tag, contexts, bodyBuf)
  }

  implicit val arbitraryMessage: Arbitrary[Message] = Arbitrary(genMessage)
}