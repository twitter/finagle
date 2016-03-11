package com.twitter.finagle.netty3

import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.util.{BufWriter, BufReader}
import com.twitter.io.{Buf, Charsets}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.openjdk.jmh.annotations._
import scala.collection.mutable.ArrayBuffer

/**
 * Benchmarks various buffer wrappers in the presence of length
 * encoded fields. This is useful since many of our protocols
 * (e.g. Mux) make heavy use of length encoding.
 */
@State(Scope.Benchmark)
class BufCodecBenchmark extends StdBenchAnnotations {
  import BufCodecBenchmark._

  @Param(Array("100"))
  var size: Int = _

  private[this] var bufs: Seq[Buf] = _
  private[this] var cbs: Seq[ChannelBuffer] = _

  private[this] var encodedCB: ChannelBuffer = _
  private[this] var encodedBuf: Buf = _

  @Setup(Level.Iteration)
  def setup(): Unit = {
    val values = List.fill(size)("value")

    bufs = values.map { case v =>
      Buf.ByteArray.Owned(v.getBytes(Charsets.Utf8))
    }

    encodedBuf = TwitterBuf.encode(bufs)

    cbs = values.map { case v =>
      ChannelBuffers.wrappedBuffer(v.getBytes(Charsets.Utf8))
    }

    encodedCB = NettyChannelBuffer.encode(cbs)
    encodedCB.markReaderIndex()
  }

  @Benchmark
  def encodeCB(): ChannelBuffer = NettyChannelBuffer.encode(cbs)

  @Benchmark
  def encodeBuf(): Buf = TwitterBuf.encode(bufs)

  @Benchmark
  def decodeCB(): Seq[ChannelBuffer] = {
    encodedCB.resetReaderIndex()
    NettyChannelBuffer.decode(encodedCB)
  }

  @Benchmark
  def decodeBuf(): Seq[Buf] = {
    TwitterBuf.decode(encodedBuf)
  }

  @Benchmark
  def roundTripCB(): Seq[ChannelBuffer] = {
    NettyChannelBuffer.decode(NettyChannelBuffer.encode(cbs))
  }

  @Benchmark
  def roundTripBuf(): Seq[Buf] = {
    TwitterBuf.decode(TwitterBuf.encode(bufs))
  }
}

object BufCodecBenchmark {
  object NettyChannelBuffer {
    def encode(values: Seq[ChannelBuffer]): ChannelBuffer = {
      var iter = values.iterator
      var size = 0
      while (iter.hasNext) {
        size += iter.next().readableBytes + 4
      }
      val cb = ChannelBuffers.buffer(size)
      iter = values.iterator
      while (iter.hasNext) {
        val v = iter.next()
        cb.writeInt(v.readableBytes)
        cb.writeBytes(v.slice())
      }
      cb
    }

    def decode(cb: ChannelBuffer): Seq[ChannelBuffer] = {
      val values = new ArrayBuffer[ChannelBuffer]
      while (cb.readableBytes() > 0) {
        val v = cb.readSlice(cb.readInt())
        values += v
      }
      values
    }
  }

  object TwitterBuf {
    def encode(values: Seq[Buf]): Buf = {
      var size = 0
      var iter = values.iterator
      while (iter.hasNext) {
        size += iter.next().length + 4
      }
      val bw = BufWriter.fixed(size)
      iter = values.iterator
      while (iter.hasNext) {
        iter.next() match { case v =>
          bw
            .writeIntBE(v.length)
            .writeBytes(Buf.ByteArray.Owned.extract(v))
        }
      }
      bw.owned()
    }

    def decode(buf: Buf): Seq[Buf] = {
      val values = new ArrayBuffer[Buf]
      val br = BufReader(buf)
      while (br.remaining > 0) {
        val v = br.readBytes(br.readIntBE())
        values += v
      }
      values
    }
  }
}