package com.twitter.finagle.mux

import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.mux.transport.Message
import org.openjdk.jmh.annotations.Benchmark

// ./sbt 'project finagle-benchmark' 'jmh:run PipelineBenchmark'
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
