package com.twitter.finagle

import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.netty4.ByteBufConversion
import com.twitter.io.{Buf, ByteReader}
import io.netty.buffer.{ByteBuf, Unpooled}
import org.openjdk.jmh.annotations.{BenchmarkMode, Measurement, Warmup, _}

/**
 * @note These tests use JMH's single shot because a `ByteReader`
 * is stateful.
 *
 * @note `ByteReader` (formerly `BufReader`) is located in util-core. This
 * benchmark remains in finagle-benchmark due to dependencies on Finagle and
 * Netty.
 */
@State(Scope.Benchmark)
class ByteReaderBenchmark extends StdBenchAnnotations {

  private[this] final val Size = 1000
  private[this] final val Iterations = 100

  private[this] final val bytes = 0.until(Size).map(_.toByte).toArray

  private[this] final val ReadBytesSize = 10
  private[this] final val ReadBytesBatchSize = Size / ReadBytesSize

  private[this] final val ReadLongBatchSize = Size / 8

  private[this] var directByteBuf: ByteBuf = _

  private[this] var heapReader: ByteReader = _
  private[this] var directReader: ByteReader = _

  private[this] def needsReset(reader: ByteReader): Boolean =
    reader == null || reader.remaining != Size

  private[this] def directByteBufNeedsReset: Boolean =
    directByteBuf == null || directByteBuf.readerIndex() > 0

  @Setup(Level.Iteration)
  def setup(): Unit = {
    if (needsReset(heapReader)) {
      val heapByteBuf = ByteBufConversion.byteBufAsBuf(Unpooled.wrappedBuffer(bytes))
      heapReader = ByteReader(heapByteBuf)
    }

    if (needsReset(directReader) || directByteBufNeedsReset) {
      val direct = Unpooled.directBuffer(Size)
      directByteBuf = direct.writeBytes(bytes)
      directReader = ByteReader(ByteBufConversion.byteBufAsBuf(directByteBuf))
    }
  }

  @TearDown(Level.Iteration)
  def tearDown(): Unit = {
    if (needsReset(directReader) || directByteBufNeedsReset)
      directByteBuf.release()
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @Measurement(iterations = Iterations, batchSize = Size)
  @Warmup(iterations = Iterations, batchSize = Size)
  def readByteHeap(): Byte =
    heapReader.readByte()

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @Measurement(iterations = Iterations, batchSize = Size)
  @Warmup(iterations = Iterations, batchSize = Size)
  def readByteDirect(): Byte =
    directReader.readByte()

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @Measurement(iterations = Iterations, batchSize = Size)
  @Warmup(iterations = Iterations, batchSize = Size)
  def readByteByteBuf(): Byte =
    directByteBuf.readByte()

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @Measurement(iterations = Iterations, batchSize = ReadBytesBatchSize)
  @Warmup(iterations = Iterations, batchSize = ReadBytesBatchSize)
  def readBytesHeap(): Buf =
    heapReader.readBytes(ReadBytesSize)

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @Measurement(iterations = Iterations, batchSize = ReadBytesBatchSize)
  @Warmup(iterations = Iterations, batchSize = ReadBytesBatchSize)
  def readBytesDirect(): Buf =
    directReader.readBytes(ReadBytesSize)

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @Measurement(iterations = Iterations, batchSize = ReadLongBatchSize)
  @Warmup(iterations = Iterations, batchSize = ReadLongBatchSize)
  def readLongHeap(): Long =
    heapReader.readLongBE()

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @Measurement(iterations = Iterations, batchSize = ReadLongBatchSize)
  @Warmup(iterations = Iterations, batchSize = ReadLongBatchSize)
  def readLongDirect(): Long =
    directReader.readLongBE()

}
