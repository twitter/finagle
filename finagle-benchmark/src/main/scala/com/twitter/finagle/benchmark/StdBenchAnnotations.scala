package com.twitter.finagle.benchmark

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._

@Fork(1)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Array(Mode.AverageTime))
@Warmup(iterations = 10)
@Measurement(iterations = 10)
abstract class StdBenchAnnotations
