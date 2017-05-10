package com.twitter.finagle.context

import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.io.Buf
import com.twitter.util.{Time, Try}
import org.openjdk.jmh.annotations.{Benchmark, Level, Scope, Setup, State}
import scala.util.Random

@State(Scope.Benchmark)
class DeadlineBenchmark extends StdBenchAnnotations {

  private[this] val N = 1024

  private[this] val bufs: Array[Buf] = {
    val rnd = new Random(21521580)
    Array.fill(N) {
      val ts = rnd.nextInt(Int.MaxValue - 1)
      val dl = rnd.nextInt(Int.MaxValue - 1)
      val deadline = Deadline(Time.fromNanoseconds(ts), Time.fromNanoseconds(dl))
      Deadline.marshal(deadline)
    }
  }

  private[this] var i = 0

  @Setup(Level.Iteration)
  def setup(): Unit =
    i = 0

  @Benchmark
  def tryUnmarhshal: Try[Deadline] = {
    i += 1
    Deadline.tryUnmarshal(bufs(i % N))
  }

}
