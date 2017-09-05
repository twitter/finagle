package com.twitter.finagle.mux.transport

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.time._
import com.twitter.finagle.liveness.Latch
import com.twitter.finagle.mux.transport.Message.Rdiscarded
import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver}
import com.twitter.finagle.transport.QueueTransport
import com.twitter.finagle.{Dentry, Dtab, Failure, Path}
import com.twitter.io.{Buf, ByteReader}
import com.twitter.util.{Await, Future, Promise, Return}
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.FunSuite
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import scala.collection.mutable.ArrayBuffer

class MuxFramerTest extends FunSuite with GeneratorDrivenPropertyChecks {
  import MuxFramerTest._

  test("writes")(forAll { (msg: Message, window: Int) =>
    whenever(window > 0) {
      val writeq = new AsyncQueue[Buf]
      // Create a queued transport that seals writes as if we've written
      // the bytes to the wire. Note, this is important since the
      // written channel buffers share buffer regions to avoid allocations.
      val transport = new QueueTransport(writeq, new AsyncQueue[Buf])
        .map({ buf: Buf =>
          deepCopy(buf)
        }, identity)

      val sr = new InMemoryStatsReceiver
      val flow = MuxFramer(transport, Some(window), sr)
      Await.result(flow.write(msg), 5.seconds)

      val payloadSize = msg.buf.length

      if (window >= payloadSize) assert(writeq.size == 1)
      else {
        val Return(q) = writeq.drain()

        // ensure that we've written the correct amount of bytes
        val written = q.foldLeft(0) { _ + _.length }
        val expected = Message.encode(msg).length
        val hdrSize = 4 * (q.size - 1)
        assert(written == expected + hdrSize)

        assert(sr.stats(Seq("write_stream_bytes")).forall { _ == window + 4 })

        q.init.foreach { buf =>
          val br = ByteReader(buf)
          br.readByte() // typ
          assert((br.readByte() >> 7 & 1) == 1) // tag MSB
        }

        val last = ByteReader(q.last)
        last.readByte() // typ
        assert((last.readByte() >> 7 & 1) == 0) // tag MSB
      }
    }
  })

  test("reads")(forAll { (msg: Message, window: Int) =>
    whenever(window > 0) {
      val transq = new AsyncQueue[Buf]
      val readLatch = new Latch

      val transport = new QueueTransport(transq, transq) {
        override def write(buf: Buf): Future[Unit] =
          super.write(deepCopy(buf))

        override def read(): Future[Buf] =
          readLatch.get.before { super.read() }
      }

      val sr = new InMemoryStatsReceiver
      val flow = MuxFramer(transport, Some(window), sr)
      Await.result(flow.write(msg), 5.seconds)

      val read = flow.read()
      readLatch.flip()

      if (msg.buf.length > window) {
        assert(sr.gauges(Seq("pending_read_streams"))() == 1)
      }

      readLatch.setDone()
      val out = Await.result(read, 5.seconds)
      assert(sr.gauges(Seq("pending_read_streams"))() == 0)

      val expected = Buf.slowHexString(Message.encode(msg))
      val received = Buf.slowHexString(Message.encode(out))
      assert(received == expected)
    }
  })

  test("write fragments disabled")(forAll { msg: Message =>
    val transq = new AsyncQueue[Buf]
    val transport = new QueueTransport(transq, transq)
    val flow = MuxFramer(transport, None, NullStatsReceiver)

    flow.write(msg)
    val out = Await.result(flow.read(), 5.seconds)
    assert(out == msg)
  })

  test("concurrent round-trip")(forAll { messages: Seq[Message] =>
    // make sure messages have unique tags.
    val msgs = messages.foldLeft(Map.empty[Int, Message]) {
      case (map, msg) => map + (msg.tag -> msg)
    }
    whenever(msgs.nonEmpty) {
      val transq = new AsyncQueue[Buf]
      val writeGate = new Promise[Unit]
      val transport = new QueueTransport(transq, transq) {
        override def write(buf: Buf): Future[Unit] = {
          writeGate.before { super.write(deepCopy(buf)) }
        }
      }

      val flow = MuxFramer(transport, Some(1), NullStatsReceiver)
      val writes = msgs.values.toSeq.map(flow.write)
      writeGate.setDone()
      Await.result(Future.collect(writes), 5.seconds)

      def go(ms: Map[Int, Message]): Future[Map[Int, Message]] =
        flow.read().flatMap { m =>
          val mms = ms + (m.tag -> m)
          if (mms.size == msgs.size) Future.value(mms) else go(mms)
        }

      assert(msgs == Await.result(go(Map.empty), 5.seconds))
    }
  })

  test("diverse stream in the presence of concurrency") {
    val msgs = (1 to 20).map { tag =>
      Message.Tdispatch(
        tag,
        Seq.empty,
        Path.read("/foo/bar/baz"),
        Dtab.empty,
        Buf.ByteArray.Owned(payload.getBytes)
      )
    }

    val transq = new AsyncQueue[Buf]
    val writtenTags = new ArrayBuffer[Int]
    val writeGate = Promise[Unit]

    val transport = new QueueTransport(transq, transq) {
      override def write(buf: Buf): Future[Unit] = {
        val br = ByteReader(buf)
        val tag = Message.Tags.extractTag(br.readIntBE())
        // clear fragment bit and store tag
        writtenTags += tag & ~Message.Tags.TagMSB
        writeGate
      }
    }

    val flow = MuxFramer(transport, Some(window), NullStatsReceiver)
    val writes = msgs.map(flow.write)
    writeGate.setDone()
    Await.result(Future.collect(writes), 5.seconds)

    // a simple run-length encoding
    def rle(xs: List[Int]): List[Int] = {
      val packed = xs.foldRight(List[List[Int]]()) { (x, rs) =>
        if (rs.isEmpty || rs.head.head != x) (x :: Nil) :: rs
        else (x :: rs.head) :: rs.tail
      }
      packed.map(_.length)
    }

    val tagRLE = rle(writtenTags.toList)
    // Ensure that a majority of the tags have a RLE of 1.
    assert(tagRLE.count(_ == 1) / tagRLE.length.toDouble >= .95)
  }

  test("client Tdiscarded") {
    val msg = Message.Tdispatch(
      20,
      Seq.empty,
      Path.read("/foo/bar/baz"),
      Dtab.empty,
      Buf.ByteArray.Owned(payload.getBytes)
    )

    val writeq = new AsyncQueue[Buf]
    val writeLatch = new Latch

    val transport = new QueueTransport(writeq, new AsyncQueue[Buf]) {
      override def write(buf: Buf): Future[Unit] = {
        writeLatch.get.before { super.write(buf) }
      }
    }

    val sr = new InMemoryStatsReceiver
    val flow = MuxFramer(transport, Some(window), sr)

    val write = flow.write(msg)
    assert(sr.gauges(Seq("pending_write_streams"))() == 1)

    flow.write(Message.Tdiscarded(20, "timeout!"))
    writeLatch.flip()
    writeLatch.flip()

    val failure = intercept[Failure] { Await.result(write, 5.seconds) }
    assert(failure.getMessage == "timeout!")

    val Return(writes) = writeq.drain()
    assert(writes.exists { buf =>
      val br = ByteReader(buf)
      val typ = Message.Tags.extractType(br.readIntBE())
      typ == Message.Types.BAD_Tdiscarded
    })
  }

  test("server Tdiscarded mid stream") {
    val msg = Message.Tdispatch(
      20,
      Seq.empty,
      Path.read("/foo/bar/baz"),
      Dtab.empty,
      Buf.ByteArray.Owned(payload.getBytes)
    )

    val readq = new AsyncQueue[Buf]
    val writeq = new AsyncQueue[Buf]
    val transport = new QueueTransport(writeq, readq)

    val sr = new InMemoryStatsReceiver
    val flow = MuxFramer(transport, Some(window), sr)

    // Offer only the first fragment
    readq.offer(Message.encodeFragments(msg, window).next())
    assert(sr.gauges(Seq("pending_read_streams"))() == 1)
    readq.offer(Message.encode(Message.Tdiscarded(20, "timeout!")))

    // still need to receive a response for this dispatch
    assert(Message.decode(Await.result(writeq.poll(), 5.seconds)) == Rdiscarded(20))
    assert(sr.gauges(Seq("pending_read_streams"))() == 0)

    // Make sure another dispatch with that tag can go through (we don't care that its too large)
    readq.offer(Message.encode(msg))
    assert(msg == Await.result(flow.read(), 5.seconds))
  }

  test("client Rdiscarded mid stream") {
    val msg = Message.RdispatchOk(
      20,
      Seq.empty,
      Buf.ByteArray.Owned(payload.getBytes)
    )

    val readq = new AsyncQueue[Buf]
    val transport = new QueueTransport(new AsyncQueue[Buf], readq)

    val sr = new InMemoryStatsReceiver
    val flow = MuxFramer(transport, Some(window), sr)

    // Offer only the first fragment
    readq.offer(Message.encodeFragments(msg, window).next())
    assert(sr.gauges(Seq("pending_read_streams"))() == 1)
    readq.offer(Message.encode(Message.Rdiscarded(20)))

    // still need to receive a response for this dispatch
    assert(Await.result(flow.read(), 5.seconds) == Rdiscarded(20))
    assert(sr.gauges(Seq("pending_read_streams"))() == 0)

    // Make sure another dispatch with that tag can go through (we don't care that its too large)
    readq.offer(Message.encode(msg))
    assert(msg == Await.result(flow.read(), 5.seconds))
  }

  test("Rdiscarded") {
    val msg = Message.RdispatchOk(20, Seq.empty, Buf.ByteArray.Owned(payload.getBytes))

    val transq = new AsyncQueue[Buf]
    val writeLatch = new Latch
    val readLatch = new Latch

    val transport = new QueueTransport(transq, transq) {
      override def write(buf: Buf): Future[Unit] = {
        writeLatch.get.before { super.write(deepCopy(buf)) }
      }

      override def read(): Future[Buf] =
        readLatch.get.before { super.read() }
    }

    val sr = new InMemoryStatsReceiver
    val flow = MuxFramer(transport, Some(window), sr)

    val write = flow.write(msg)
    val read = flow.read()

    writeLatch.flip()
    readLatch.flip()
    assert(sr.gauges(Seq("pending_read_streams"))() == 1)
    assert(sr.gauges(Seq("pending_write_streams"))() == 1)

    val exc = Failure("timeout!")
    write.raise(exc)
    writeLatch.flip()
    assert(exc == intercept[Failure] { Await.result(write, 5.second) })

    writeLatch.setDone()
    readLatch.setDone()

    assert(sr.gauges(Seq("pending_read_streams"))() == 0)
    assert(sr.gauges(Seq("pending_write_streams"))() == 0)
  }

  test("header endec")(forAll(Gen.choose(1, Int.MaxValue)) { size: Int =>
    assert(MuxFramer.Header.decodeFrameSize(MuxFramer.Header.encodeFrameSize(size)) == size)
  })
}

object MuxFramerTest {
  val window = 1 << 6
  val payload = ("a" * window) * 10

  def deepCopy(b: Buf): Buf = Buf.ByteArray.Owned(Buf.ByteArray.Shared.extract(b))

  val genMessage: Gen[Message] = for {
    typ <- Gen.oneOf(Seq(Message.Types.Tdispatch, Message.Types.Rdispatch))
    tag <- Gen.choose(Message.Tags.MinTag, Message.Tags.MaxTag)
    ctx <- Gen.alphaStr
    path <- Gen.oneOf(Seq("/", "/okay", "/foo/bar/baz"))
    dentry <- Gen.oneOf(Seq("/a=>/b", "/foo=>/$/inet/twitter.com/80"))
    body <- Gen.alphaStr
  } yield {
    val ctxBuf = Buf.Utf8(ctx)
    val contexts = Seq(ctxBuf -> ctxBuf)
    val bodyBuf = Buf.Utf8(body)
    if (typ == Message.Types.Tdispatch)
      Message.Tdispatch(
        tag,
        contexts,
        Path.read(path),
        Dtab(IndexedSeq(Dentry.read(dentry))),
        bodyBuf
      )
    else
      Message.RdispatchOk(tag, contexts, bodyBuf)
  }

  implicit val arbitraryMessage: Arbitrary[Message] = Arbitrary(genMessage)
}
