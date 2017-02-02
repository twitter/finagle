package com.twitter.finagle.util

import com.twitter.finagle.benchmark.StdBenchAnnotations
import org.openjdk.jmh.annotations._

/**
 * Note: These tests use JMH's single shot because a BufWriter
 * is stateful.
 */
@State(Scope.Benchmark)
class BufWriterBenchmark extends StdBenchAnnotations {

  private[this] final val Size = 1000

  private[this] final val Iterations = 50

  private[this] final val NumBytes = 5
  private[this] final val Bytes = 0.until(NumBytes).map(_.toByte).toArray

  private[this] final val WriteBytesBatchSize = Size / NumBytes

  private[this] val bytesOut = new Array[Byte](Size)

  private[this] var bufWriter: BufWriter = _

  @Setup(Level.Iteration)
  def setup(): Unit =
    bufWriter = BufWriter(bytesOut)

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @Measurement(iterations = Iterations, batchSize = Size)
  @Warmup(iterations = Iterations, batchSize = Size)
  def writeByteFixed(): BufWriter =
    bufWriter.writeByte(0x5)

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @Measurement(iterations = Iterations, batchSize = WriteBytesBatchSize)
  @Warmup(iterations = Iterations, batchSize = WriteBytesBatchSize)
  def writeBytesFixed(): BufWriter =
    bufWriter.writeBytes(Bytes)

}
