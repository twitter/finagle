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

  private[this] var channelBufferBuf: Buf = _

  private[this] var all: Array[Buf] = _

  @Setup(Level.Iteration)
  def setup(): Unit = {
    val cap = size * 2
    val start = cap / 4
    val end = start + size
    val raw = 0.until(cap).map(_.toByte).toArray

    val bb = java.nio.ByteBuffer.wrap(raw, start, size)
    val cb = ChannelBuffers.wrappedBuffer(raw, start, size)
    channelBufferBuf = ChannelBufferBuf.Owned(cb)

    val byteArrayBuf = Buf.ByteArray.Owned(raw, start, end)
    val byteBufferBuf = Buf.ByteBuffer.Owned(bb)
    val concatBuf = byteArrayBuf.slice(0, size / 2).concat(byteArrayBuf.slice(size / 2, size))
    all = Array(byteArrayBuf, byteBufferBuf, concatBuf, channelBufferBuf)
  }

  @Benchmark
  def equality(hole: Blackhole): Unit = {
    var i = 0
    while (i < all.length) {
      hole.consume(channelBufferBuf == all(i))
      hole.consume(all(i) == channelBufferBuf)
      i += 1
    }
  }

  @Benchmark
  def hash(): Int =
    channelBufferBuf.hashCode

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
