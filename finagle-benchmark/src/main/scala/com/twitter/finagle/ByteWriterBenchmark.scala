package com.twitter.finagle

import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.io.{BufByteWriter, ByteWriter}
import org.openjdk.jmh.annotations._

/**
 * @note These tests use JMH's single shot because a `ByteWriter`
 * is stateful.
 *
 * @note `ByteWriter` (formerly `BufWriter`) is located in util-core. This
 * benchmark remains in finagle-benchmark to keep it co-located with
 * `ByteReaderBenchmark`. `ByteReaderBenchmark` has dependencies on Finagle
 * and Netty, so needs to remain in finagle-benchmark.
 */
@State(Scope.Benchmark)
class ByteWriterBenchmark extends StdBenchAnnotations {

  private[this] final val Size = 1000

  private[this] final val Iterations = 50

  private[this] final val NumBytes = 5
  private[this] final val Bytes = 0.until(NumBytes).map(_.toByte).toArray

  private[this] final val WriteBytesBatchSize = Size / NumBytes

  private[this] val bytesOut = new Array[Byte](Size)

  private[this] var byteWriter: ByteWriter = _

  @Setup(Level.Iteration)
  def setup(): Unit =
    byteWriter = BufByteWriter(bytesOut)

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @Measurement(iterations = Iterations, batchSize = Size)
  @Warmup(iterations = Iterations, batchSize = Size)
  def writeByteFixed(): ByteWriter =
    byteWriter.writeByte(0x5)

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @Measurement(iterations = Iterations, batchSize = WriteBytesBatchSize)
  @Warmup(iterations = Iterations, batchSize = WriteBytesBatchSize)
  def writeBytesFixed(): ByteWriter =
    byteWriter.writeBytes(Bytes)

}
