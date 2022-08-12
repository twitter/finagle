package com.twitter.finagle.mux

import com.twitter.io.Buf
import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.mux.transport.Message
import org.openjdk.jmh.annotations._

// ./bazel run //finagle/finagle-benchmark/src/main/scala:jmh 'PipelineBenchmark'
class MessageBenchmark extends StdBenchAnnotations {

  @Benchmark
  def RreqOk(): Message = roundTrip(Workload.RreqOk)

  @Benchmark
  def RreqError(): Message = roundTrip(Workload.RreqError)

  @Benchmark
  def RreqNack(): Message = roundTrip(Workload.RreqNack)

  @Benchmark
  def Tdispatch(): Message = roundTrip(Workload.Tdispatch)

  @Benchmark
  def RdispatchOk(): Message = roundTrip(Workload.RdispatchOk)

  @Benchmark
  def RdispatchError(): Message = roundTrip(Workload.RdispatchError)

  @Benchmark
  def RdispatchNack(): Message = roundTrip(Workload.RdispatchNack)

  @Benchmark
  def Tdrain(): Message = roundTrip(Workload.Tdrain)

  @Benchmark
  def Rdrain(): Message = roundTrip(Workload.Rdrain)

  @Benchmark
  def Tping(): Message = roundTrip(Workload.Tping)

  @Benchmark
  def Rping(): Message = roundTrip(Workload.Rping)

  @Benchmark
  def Tdiscarded(): Message = roundTrip(Workload.Tdiscarded)

  @Benchmark
  def Rdiscarded(): Message = roundTrip(Workload.Rdiscarded)

  @Benchmark
  def Tlease(): Message = roundTrip(Workload.Tlease)

  @Benchmark
  def Tinit(): Message = roundTrip(Workload.Tinit)

  @Benchmark
  def Rinit(): Message = roundTrip(Workload.Rinit)

  @Benchmark
  def Rerr(): Message = roundTrip(Workload.Rerr)

  @Benchmark
  def Treq(): Message = roundTrip(Workload.TReq)

  private def roundTrip(m: Message): Message = Message.decode(Message.encode(m))
}

// ./sbt 'project finagle-benchmark' 'jmh:run EncodeMessageBenchmark'
class EncodeMessageBenchmark extends StdBenchAnnotations {

  @Benchmark
  def RreqOk(): Buf = encode(Workload.RreqOk)

  @Benchmark
  def RreqError(): Buf = encode(Workload.RreqError)

  @Benchmark
  def RreqNack(): Buf = encode(Workload.RreqNack)

  @Benchmark
  def Tdispatch(): Buf = encode(Workload.Tdispatch)

  @Benchmark
  def RdispatchOk(): Buf = encode(Workload.RdispatchOk)

  @Benchmark
  def RdispatchError(): Buf = encode(Workload.RdispatchError)

  @Benchmark
  def RdispatchNack(): Buf = encode(Workload.RdispatchNack)

  @Benchmark
  def Tdrain(): Buf = encode(Workload.Tdrain)

  @Benchmark
  def Rdrain(): Buf = encode(Workload.Rdrain)

  @Benchmark
  def Tping(): Buf = encode(Workload.Tping)

  @Benchmark
  def Rping(): Buf = encode(Workload.Rping)

  @Benchmark
  def Tdiscarded(): Buf = encode(Workload.Tdiscarded)

  @Benchmark
  def Rdiscarded(): Buf = encode(Workload.Rdiscarded)

  @Benchmark
  def Tlease(): Buf = encode(Workload.Tlease)

  @Benchmark
  def Tinit(): Buf = encode(Workload.Tinit)

  @Benchmark
  def Rinit(): Buf = encode(Workload.Rinit)

  @Benchmark
  def Rerr(): Buf = encode(Workload.Rerr)

  @Benchmark
  def Treq(): Buf = encode(Workload.TReq)

  private def encode(m: Message): Buf = Message.encode(m)
}

// ./sbt 'project finagle-benchmark' 'jmh:run DecodeMessageBenchmark'
@State(Scope.Benchmark)
class DecodeMessageBenchmark extends StdBenchAnnotations {

  // We don't decode composite `Buf`s in Mux, so transform each encoded
  // `Message` into a plain `Buf.ByteArray`.
  val RreqOkBuf: Buf = Buf.ByteArray.coerce(Message.encode(Workload.RreqOk))
  val RreqErrorBuf: Buf = Buf.ByteArray.coerce(Message.encode(Workload.RreqError))
  val RreqNackBuf: Buf = Buf.ByteArray.coerce(Message.encode(Workload.RreqNack))
  val TdispatchBuf: Buf = Buf.ByteArray.coerce(Message.encode(Workload.Tdispatch))
  val RdispatchOkBuf: Buf = Buf.ByteArray.coerce(Message.encode(Workload.RdispatchOk))
  val RdispatchErrorBuf: Buf = Buf.ByteArray.coerce(Message.encode(Workload.RdispatchError))
  val RdispatchNackBuf: Buf = Buf.ByteArray.coerce(Message.encode(Workload.RdispatchNack))
  val TdrainBuf: Buf = Buf.ByteArray.coerce(Message.encode(Workload.Tdrain))
  val RdrainBuf: Buf = Buf.ByteArray.coerce(Message.encode(Workload.Rdrain))
  val TpingBuf: Buf = Buf.ByteArray.coerce(Message.encode(Workload.Tping))
  val RpingBuf: Buf = Buf.ByteArray.coerce(Message.encode(Workload.Rping))
  val TdiscardedBuf: Buf = Buf.ByteArray.coerce(Message.encode(Workload.Tdiscarded))
  val RdiscardedBuf: Buf = Buf.ByteArray.coerce(Message.encode(Workload.Rdiscarded))
  val TleaseBuf: Buf = Buf.ByteArray.coerce(Message.encode(Workload.Tlease))
  val TinitBuf: Buf = Buf.ByteArray.coerce(Message.encode(Workload.Tinit))
  val RinitBuf: Buf = Buf.ByteArray.coerce(Message.encode(Workload.Rinit))
  val RerrBuf: Buf = Buf.ByteArray.coerce(Message.encode(Workload.Rerr))
  val TReqBuf: Buf = Buf.ByteArray.coerce(Message.encode(Workload.TReq))

  @Benchmark
  def RreqOk(): Message = decode(RreqOkBuf)

  @Benchmark
  def RreqError(): Message = decode(RreqErrorBuf)

  @Benchmark
  def RreqNack(): Message = decode(RreqNackBuf)

  @Benchmark
  def Tdispatch(): Message = decode(TdispatchBuf)

  @Benchmark
  def RdispatchOk(): Message = decode(RdispatchOkBuf)

  @Benchmark
  def RdispatchError(): Message = decode(RdispatchErrorBuf)

  @Benchmark
  def RdispatchNack(): Message = decode(RdispatchNackBuf)

  @Benchmark
  def Tdrain(): Message = decode(TdrainBuf)

  @Benchmark
  def Rdrain(): Message = decode(RdrainBuf)

  @Benchmark
  def Tping(): Message = decode(TpingBuf)

  @Benchmark
  def Rping(): Message = decode(RpingBuf)

  @Benchmark
  def Tdiscarded(): Message = decode(TdiscardedBuf)

  @Benchmark
  def Rdiscarded(): Message = decode(RdiscardedBuf)

  @Benchmark
  def Tlease(): Message = decode(TleaseBuf)

  @Benchmark
  def Tinit(): Message = decode(TinitBuf)

  @Benchmark
  def Rinit(): Message = decode(RinitBuf)

  @Benchmark
  def Rerr(): Message = decode(RerrBuf)

  @Benchmark
  def Treq(): Message = decode(TReqBuf)

  private def decode(m: Buf): Message = Message.decode(m)
}
