package com.twitter.finagle.mux

import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.mux.{transport => mux}
import com.twitter.finagle.netty4.ByteBufAsBuf
import com.twitter.finagle.{Dtab, Path}
import com.twitter.io.Buf
import com.twitter.util.Time
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.embedded.EmbeddedChannel
import org.openjdk.jmh.annotations._

// ./sbt 'project finagle-benchmark' 'jmh:run DecodePipelineBenchmark'
@State(Scope.Benchmark)
class DecodePipelineBenchmark extends StdBenchAnnotations {

  val data: Buf = Buf.ByteArray.Owned(Array[Byte](1, 2, 3))

  @Setup(Level.Iteration)
  def setup(): Unit = {
    channel = new EmbeddedChannel()
    mux.RefCountingFramer(channel.pipeline)
  }


  val RreqOk = msgToDirectByteBuf(mux.Message.RreqOk(0, data))
  @Benchmark
  def decodeRreqOk(): mux.Message = decode(RreqOk)

  val RreqError = msgToDirectByteBuf(mux.Message.RreqError(0, "bad news"))
  @Benchmark
  def decodeRreqError(): mux.Message = decode(RreqError)

  val RreqNack = msgToDirectByteBuf(mux.Message.RreqNack(0))
  @Benchmark
  def decodeRreqNack(): mux.Message = decode(RreqNack)

  val Tdispatch = msgToDirectByteBuf(mux.Message.Tdispatch(0, Seq.empty, Path.empty, Dtab.empty, data))
  @Benchmark
  def decodeTdispatch(): mux.Message = decode(Tdispatch)

  val RdispatchOk = msgToDirectByteBuf(mux.Message.RdispatchOk(0, Seq.empty, data))
  @Benchmark
  def decodeRdispatchOk(): mux.Message = decode(RdispatchOk)

  val RdispatchError = msgToDirectByteBuf(mux.Message.RdispatchError(0, Seq.empty, "bad"))
  @Benchmark
  def decodeRdispatchError(): mux.Message = decode(RdispatchError)

  val RdispatchNack = msgToDirectByteBuf(mux.Message.RdispatchNack(0, Seq.empty))
  @Benchmark
  def decodeRdispatchNack(): mux.Message = decode(RdispatchNack)

  val Tdrain = msgToDirectByteBuf(mux.Message.Tdrain(0))
  @Benchmark
  def decodeTdrain(): mux.Message = decode(Tdrain)

  val Rdrain = msgToDirectByteBuf(mux.Message.Rdrain(0))
  @Benchmark
  def decodeRdrain(): mux.Message = decode(Rdrain)

  val Tping = msgToDirectByteBuf(mux.Message.Tping(0))
  @Benchmark
  def decodeTping(): mux.Message = decode(Tping)

  val Rping = msgToDirectByteBuf(mux.Message.Rping(0))
  @Benchmark
  def decodeRping(): mux.Message = decode(Rping)

  val Tdiscarded = msgToDirectByteBuf(mux.Message.Tdiscarded(0, "give up already"))
  @Benchmark
  def decodeTdiscarded(): mux.Message = decode(Tdiscarded)

  val Rdiscarded = msgToDirectByteBuf(mux.Message.Rdiscarded(0))
  @Benchmark
  def decodeRdiscarded(): mux.Message = decode(Rdiscarded)

  val Tlease = msgToDirectByteBuf(mux.Message.Tlease(Time.now))
  @Benchmark
  def decodeTlease(): mux.Message = decode(Tlease)

  val Tinit = msgToDirectByteBuf(mux.Message.Tinit(0, 0, Seq.empty))
  @Benchmark
  def decodeTinit(): mux.Message = decode(Tinit)

  val Rinit = msgToDirectByteBuf(mux.Message.Rinit(0, 0, Seq.empty))
  @Benchmark
  def decodeRinit(): mux.Message = decode(Rinit)

  val Rerr = msgToDirectByteBuf(mux.Message.Rerr(0, "bad news"))
  @Benchmark
  def decodeRerr(): mux.Message = decode(Rerr)

  val TReq = msgToDirectByteBuf(mux.Message.Treq(0, None, data))
  @Benchmark
  def decodeTreq(): mux.Message = decode(TReq)

  var channel: EmbeddedChannel = null

  def decode(msg: ByteBuf): mux.Message = {
    msg.readerIndex(0)
    msg.retain()
    channel.writeInbound(msg)
    mux.Message.decode(channel.readInbound[ByteBufAsBuf]())
  }

  def msgToDirectByteBuf(msg: mux.Message): ByteBuf = {
    val buf = mux.Message.encode(msg)
    val bytes = Buf.ByteArray.Owned.extract(Buf.U32BE(buf.length).concat(buf))
    val d = Unpooled.directBuffer(bytes.length)
    d.writeBytes(bytes)
    d
  }
}
