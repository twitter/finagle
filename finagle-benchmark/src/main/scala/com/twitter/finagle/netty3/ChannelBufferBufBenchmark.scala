package com.twitter.finagle.netty3

import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.io.Buf
import java.nio
import org.jboss.netty.buffer.ChannelBuffers
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@State(Scope.Benchmark)
class ChannelBufferBufBenchmark extends StdBenchAnnotations {

  @Param(Array("1000"))
  var size: Int = 1000

  private[this] var bytes: Array[Byte] = _
  private[this] var channelBufferBuf: Buf = _
  private[this] var byteArrayBuf: Buf = _
  private[this] var concatBuf: Buf = _

  @Setup(Level.Iteration)
  def setup(): Unit = {
    val cap = size * 2
    val start = cap / 4
    val end = start + size
    bytes = 0.until(cap).map(_.toByte).toArray

    val bb = java.nio.ByteBuffer.wrap(bytes, start, size)
    val cb = ChannelBuffers.wrappedBuffer(bytes, start, size)
    channelBufferBuf = ChannelBufferBuf.Owned(cb)
    byteArrayBuf = Buf.ByteArray.Owned(bytes, start, end)
    concatBuf = byteArrayBuf.slice(0, size / 2).concat(byteArrayBuf.slice(size / 2, size))
  }

  @Benchmark
  def equalityChannelBufferBufChannelBufferBuf(): Boolean =
    channelBufferBuf == channelBufferBuf

  @Benchmark
  def equalityChannelBufferBufByteArray(): Boolean =
    channelBufferBuf == byteArrayBuf

  @Benchmark
  def equalityChannelBufferBufConcat(): Boolean =
    channelBufferBuf == concatBuf

  @Benchmark
  def equalityByteArrayChannelBufferBuf(): Boolean =
    byteArrayBuf == channelBufferBuf

  @Benchmark
  def equalityConcatChannelBufferBuf(): Boolean =
    concatBuf == channelBufferBuf

  @Benchmark
  @Warmup(iterations = 5)
  @Measurement(iterations = 5)
  def hashCodeBaseline(): Buf =
    ChannelBufferBuf.Owned(ChannelBuffers.wrappedBuffer(bytes, 1, size + 1))

  // subtract the results of the Baseline run to get the results
  @Benchmark
  @Warmup(iterations = 5)
  @Measurement(iterations = 5)
  def hashCode(hole: Blackhole): Int = {
    val buf = hashCodeBaseline()
    hole.consume(buf)
    buf.hashCode
  }

  @Benchmark
  def slice(): Buf =
    channelBufferBuf.slice(size / 4, size / 4 + size / 2)

  @Benchmark
  def extractByteBuffer(): nio.ByteBuffer =
    Buf.ByteBuffer.Owned.extract(channelBufferBuf)

  @Benchmark
  def extractByteArray(): Array[Byte] =
    Buf.ByteArray.Owned.extract(channelBufferBuf)

  @Benchmark
  def length(): Int =
    channelBufferBuf.length

}
