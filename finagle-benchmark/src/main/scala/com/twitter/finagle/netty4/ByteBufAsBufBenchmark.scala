package com.twitter.finagle.netty4

import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.io.Buf
import io.netty.buffer.Unpooled
import java.nio
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@State(Scope.Benchmark)
class ByteBufAsBufBenchmark extends StdBenchAnnotations {

  @Param(Array("1000"))
  var size: Int = 1000

  private[this] var bytes: Array[Byte] = _
  private[this] var byteBuf: Buf = _
  private[this] var byteBufDirect: Buf = _
  private[this] var byteArrayBuf: Buf = _
  private[this] var concatBuf: Buf = _

  @Setup(Level.Iteration)
  def setup(): Unit = {
    val cap = size * 2
    val start = cap / 4
    val end = start + size
    bytes = 0.until(cap).map(_.toByte).toArray

    byteBuf = ByteBufAsBuf.Owned(Unpooled.wrappedBuffer(bytes, start, size))
    byteArrayBuf = Buf.ByteArray.Owned(bytes, start, end)
    concatBuf = byteArrayBuf.slice(0, size / 2).concat(byteArrayBuf.slice(size / 2, size))

    val direct = Unpooled.directBuffer(cap)
    direct.writeBytes(bytes, start, size)
    byteBufDirect = ByteBufAsBuf.Owned(direct)
  }

  @TearDown(Level.Iteration)
  def tearDown(): Unit =
    ByteBufAsBuf.Owned.extract(byteBufDirect).release()

  @Benchmark
  def equalityByteBufByteBuf(): Boolean =
    byteBuf == byteBuf

  @Benchmark
  def equalityByteBufByteArray(): Boolean =
    byteBuf == byteArrayBuf

  @Benchmark
  def equalityByteBufConcat(): Boolean =
    byteBuf == concatBuf

  @Benchmark
  def equalityByteArrayByteBuf(): Boolean =
    byteArrayBuf == byteBuf

  @Benchmark
  def equalityConcatByteBuf(): Boolean =
    concatBuf == byteBuf

  @Benchmark
  def equalityDirectByteBufByteArray(): Boolean =
    byteBufDirect == byteArrayBuf

  @Benchmark
  def equalityByteArrayDirectByteBuf(): Boolean =
    byteArrayBuf == byteBufDirect

  @Benchmark
  @Warmup(iterations = 5)
  @Measurement(iterations = 5)
  def hashCodeBaseline(): Buf =
    ByteBufAsBuf.Owned(Unpooled.wrappedBuffer(bytes, 1, size + 1))

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
    byteBuf.slice(size / 4, size / 4 + size / 2)

  @Benchmark
  def extractByteBuffer(): nio.ByteBuffer =
    Buf.ByteBuffer.Owned.extract(byteBuf)

  @Benchmark
  def extractByteArray(): Array[Byte] =
    Buf.ByteArray.Owned.extract(byteBuf)

  @Benchmark
  def length(): Int =
    byteBuf.length

}

