package com.twitter.finagle.http

import com.twitter.finagle.benchmark.StdBenchAnnotations
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole
import scala.util.Random

@State(Scope.Benchmark)
class CookieMapBenchmark extends StdBenchAnnotations {

  protected def newMap(): CookieMap = new CookieMap(Request())

  // We supply 20 random strings of the length of 14 and build a 10-element cookie map of them.
  private val map = Iterator
    .fill(10 * 2)(Random.alphanumeric.take(14).mkString)
    .grouped(2)
    .foldLeft(newMap()) { (map, h) => map += h.head -> new Cookie(h.head, h.last) }

  @Benchmark
  def iterate(b: Blackhole): Unit = map.foreach(c => b.consume(c))
}
