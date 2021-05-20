package com.twitter.finagle.decoder

import com.twitter.io.Buf
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import scala.util.Random
import org.scalatest.funsuite.AnyFunSuite

class LengthFieldFramerTest extends AnyFunSuite with ScalaCheckDrivenPropertyChecks {

  val MaxTestFrameSize = 32
  val MaxTestFrames = 30

  var counter = -1
  def mkByte(): Byte = {
    counter += 1
    (counter & 0xff).toByte
  }

  def mkBuf(bytes: Byte*): Buf = Buf.ByteArray.Owned(bytes.toArray)

  // Make a simple frame consisting of a one-byte size followed by data
  def mkTestFrame(dataSize: Int): Buf = {
    Buf.ByteArray
      .Owned(Array[Byte]((dataSize & 0xff).toByte))
      .concat(Buf.ByteArray.Owned(Array.fill[Byte](dataSize)(mkByte())))
  }

  def validateFrame(buf: Buf, expectedSize: Int): Unit = {
    val sizeByte = Array[Byte](1)
    buf.slice(0, 1).write(sizeByte, 0)
    val dataSize = sizeByte(0) & 0xff
    assert(buf.length == dataSize + 1)
    assert(dataSize == expectedSize)
  }

  // Partition a stream randomly in to chunks of at least one byte and at most
  // (MaxTestFrameSize * 2 + 10)
  def partitionStream(buf: Buf, rand: Random): Seq[Buf] = {
    val split = rand.nextInt(MaxTestFrameSize * 2) + 1
    if (split < buf.length) {
      buf.slice(0, split) +: partitionStream(buf.slice(split, buf.length), rand)
    } else {
      Seq(buf)
    }
  }

  // Generate a stream of frames of random sizes
  def dataStream(rand: Random): (Seq[Int], Buf) = {
    val frames = rand.nextInt(MaxTestFrames) + 1
    val frameSizes = (1 to frames).map(_ => rand.nextInt(MaxTestFrameSize))
    val stream = frameSizes
      .map { size => mkTestFrame(size) }
      .foldLeft(Buf.Empty)(_.concat(_))
    (frameSizes, stream)
  }

  test("Accumulates partial frames properly and preserves frame ordering")(forAll { seed: Int =>
    val rand = new Random(seed)
    val (sizes, stream) = dataStream(rand)

    assert(sizes.sum + sizes.length == stream.length)

    val decoder = new LengthFieldFramer(
      lengthFieldBegin = 0,
      lengthFieldLength = 1,
      lengthAdjust = 1,
      maxFrameLength = 64,
      bigEndian = true
    )

    val decodedStream = partitionStream(stream, rand).flatMap(decoder)

    assert(decodedStream.length == sizes.length)

    // Make sure the frames are all there and have the correct length
    decodedStream.zip(sizes).foreach {
      case (frame: Buf, size: Int) =>
        validateFrame(frame, size)
    }

    // Make sure frames are delivered in order
    assert(decodedStream.fold(Buf.Empty)(_.concat(_)) == stream)
  })

  test("behave properly for frames that report zero size") {
    // [1 byte magic][2 bytes size of data][data]
    val packetSizeOne = mkBuf(0x11, 0x00, 0x01, 0x0d)
    val packetSizeZero = mkBuf(0x11, 0x00, 0x00)

    val decoder = new LengthFieldFramer(
      lengthFieldBegin = 1,
      lengthFieldLength = 2,
      lengthAdjust = 3,
      maxFrameLength = 64,
      bigEndian = true
    )

    assert(decoder(packetSizeZero) == IndexedSeq(packetSizeZero))

    // Hedge against a situation where the cursor is not moved by a zero-sized Frame.
    assert(decoder(packetSizeOne) == IndexedSeq(packetSizeOne))
  }

  test(
    "behave properly if the length field is in the middle and the given length counts the header"
  ) {
    //               { header     len  header}{ data                     }
    val frame = mkBuf(0x0a, 0x0a, 0x09, 0x0a, 0x0d, 0x0d, 0x0d, 0x0d, 0x0d)
    val decoder = new LengthFieldFramer(
      lengthFieldBegin = 2,
      lengthFieldLength = 1,
      lengthAdjust = 0,
      maxFrameLength = 64,
      bigEndian = true
    )

    assert(decoder(frame.slice(0, 5)).isEmpty)
    assert(decoder(frame.slice(5, 9)).head == frame)
  }

  test("throw FrameTooLargeException when a frame exceeds maxFrameLength") {
    val frame = mkBuf(0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00)
    val decoder = new LengthFieldFramer(
      lengthFieldBegin = 0,
      lengthFieldLength = 1,
      lengthAdjust = 1,
      maxFrameLength = 8,
      bigEndian = true
    )

    intercept[LengthFieldFramer.FrameTooLargeException] {
      decoder(frame)
    }
  }

  def mk(begin: Int = 0, len: Int = 1, adj: Int = 1, max: Int = 64) = {
    new LengthFieldFramer(begin, len, adj, max, bigEndian = true)
  }

  test("validate field length") {
    intercept[IllegalArgumentException] { mk(len = 5) }
  }

  test("validate field begin") {
    intercept[IllegalArgumentException] { mk(begin = -1) }
  }

  test("check length field falls between 0 and maxFrameLength") {
    intercept[IllegalArgumentException] { mk(begin = 60, len = 8, max = 64) }
  }

  test("validate length adjust") {
    intercept[IllegalArgumentException] { mk(adj = -1) }
    intercept[IllegalArgumentException] { mk(adj = 100) }
  }

  test("handle multi-byte sizes in big endian order") {
    def decode(buf: Buf, size: Int) = {
      val decoder = new LengthFieldFramer(
        lengthFieldBegin = 0,
        lengthFieldLength = size,
        lengthAdjust = size,
        maxFrameLength = 10,
        bigEndian = true
      )

      decoder(buf).head
    }

    val f2BE = mkBuf(0x00, 0x02, 0x11, 0x11)
    val f3BE = mkBuf(0x00, 0x00, 0x02, 0x11, 0x11)
    val f4BE = mkBuf(0x00, 0x00, 0x00, 0x02, 0x11, 0x11)
    val f8BE = mkBuf(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x11, 0x11)

    assert(decode(f2BE, 2) == f2BE)
    assert(decode(f3BE, 3) == f3BE)
    assert(decode(f4BE, 4) == f4BE)
    assert(decode(f8BE, 8) == f8BE)
  }

  test("handle multi-byte sizes in little endian order") {
    def decode(buf: Buf, size: Int) = {
      val decoder = new LengthFieldFramer(
        lengthFieldBegin = 0,
        lengthFieldLength = size,
        lengthAdjust = size,
        maxFrameLength = 10,
        bigEndian = false
      )

      decoder(buf).head
    }

    val f2LE = mkBuf(0x02, 0x00, 0x11, 0x11)
    val f3LE = mkBuf(0x02, 0x00, 0x00, 0x11, 0x11)
    val f4LE = mkBuf(0x02, 0x00, 0x00, 0x00, 0x11, 0x11)
    val f8LE = mkBuf(0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x11, 0x11)

    assert(decode(f2LE, 2) == f2LE)
    assert(decode(f3LE, 3) == f3LE)
    assert(decode(f4LE, 4) == f4LE)
    assert(decode(f8LE, 8) == f8LE)
  }
}
