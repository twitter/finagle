package com.twitter.finagle.http

import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.http.netty3.Netty3HeaderMap
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole
import scala.util.Random

@State(Scope.Benchmark)
abstract class HeaderMapBenchmark extends StdBenchAnnotations {

  protected def newMap(): HeaderMap

  @Param(Array("0", "10"))
  private var size: Int = _

  private var map: HeaderMap = _

  @Setup(Level.Invocation)
  def setup(): Unit = {
    map = Iterator
      .fill(size * 2)(Random.alphanumeric.take(3).mkString)
      .grouped(2)
      .foldLeft(newMap())((map, h) => map.add(h.head, h.last))
  }

  @Benchmark
  def add(): HeaderMap = map.add("foo", "bar")

  @Benchmark
  def iterate(b: Blackhole): Unit = map.foreach(h => b.consume(h))
}

class MapHeaderMapBenchmark extends HeaderMapBenchmark {
  protected def newMap(): HeaderMap = MapHeaderMap()
}

class N3HeaderMapBenchmark extends HeaderMapBenchmark {
  protected def newMap(): HeaderMap = new Netty3HeaderMap()
}

