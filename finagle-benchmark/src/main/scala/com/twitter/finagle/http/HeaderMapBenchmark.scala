package com.twitter.finagle.http

import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.http.headers.JTreeMapBackedHeaderMap
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole
import scala.util.Random

@State(Scope.Benchmark)
abstract class HeaderMapBenchmark extends StdBenchAnnotations {

  protected def newMap(): HeaderMap

  // We supply 18 random strings of the length of 14 and build a 9-element
  // header map of them. The 10th element is foo -> bar so we can reliably
  // query it in the benchmark.
  private val randommap = Iterator
    .fill(9 * 2)(Random.alphanumeric.take(14).mkString)
    .grouped(2)
    .foldLeft(newMap())((map, h) => map.add(h.head, h.last))
    .add("Content-Length", "100")

  private val duplicateKeyMap =
    (0 until 10).foldLeft(newMap())((map, _) => map.add("key'", "value"))

  private val nonrandommap = newMap()
    .add("access-control-allow-credentials", "true")
    .add("access-control-allow-origin", "https://twitter.com")
    .add("cache-control", "no-cache, no-store, must-revalidate, pre-check=0, post-check=0")
    .add("content-disposition", "attachment; filename=json.json")
    .add("content-encoding", "gzip")
    .add("content-type", "application/json;charset=utf-8")
    .add("date", "Fri, 20 Sep 2019 14:12:06 GMT")
    .add("expires", "Tue, 31 Mar 1981 05:00:00 GMT")
    .add("last-modified", "Fri, 20 Sep 2019 14:12:05 GMT")
    .add("pragma", "no-cache")
    .add("server", "tsa_o")
    .add("status", "200, 200 OK")
    .add("strict-transport-security", "max-age=631138519")
    .add("x-access-level", "read-write-directmessages")
    .add("x-client-event-enabled", "true")
    .add("x-connection-hash", "0bce97311cd0d4e0070842966f5bd9ab")
    .add("x-content-type-options", "nosniff")
    .add("x-frame-options", "SAMEORIGIN")
    .add("x-response-time", "116")
    .add("x-transaction", "00977e9800e16948")
    .add("x-tsa-request-body-time", "0")
    .add("x-twitter-response-tags", "BouncerCompliant")
    .add("x-xss-protection", "0")
    .add("Content-Length", "100")

  @Benchmark
  def create(): HeaderMap = newMap()

  @Benchmark
  def getRandom(): Option[String] = randommap.get("Content-Length")

  @Benchmark
  def getRealistic(): Option[String] = nonrandommap.get("Content-Length")

  @Benchmark
  def createAndAdd(): HeaderMap = newMap().add("Content-Length", "100")

  @Benchmark
  @Warmup(iterations = 15)
  def createAndAdd24(): HeaderMap = {
    newMap()
      .add("access-control-allow-credentials", "true")
      .add("access-control-allow-origin", "https://twitter.com")
      .add("cache-control", "no-cache, no-store, must-revalidate, pre-check=0, post-check=0")
      .add("content-disposition", "attachment; filename=json.json")
      .add("content-encoding", "gzip")
      .add("content-type", "application/json;charset=utf-8")
      .add("date", "Fri, 20 Sep 2019 14:12:06 GMT")
      .add("expires", "Tue, 31 Mar 1981 05:00:00 GMT")
      .add("last-modified", "Fri, 20 Sep 2019 14:12:05 GMT")
      .add("pragma", "no-cache")
      .add("server", "tsa_o")
      .add("status", "200, 200 OK")
      .add("strict-transport-security", "max-age=631138519")
      .add("x-access-level", "read-write-directmessages")
      .add("x-client-event-enabled", "true")
      .add("x-connection-hash", "0bce97311cd0d4e0070842966f5bd9ab")
      .add("x-content-type-options", "nosniff")
      .add("x-frame-options", "SAMEORIGIN")
      .add("x-response-time", "116")
      .add("x-transaction", "00977e9800e16948")
      .add("x-tsa-request-body-time", "0")
      .add("x-twitter-response-tags", "BouncerCompliant")
      .add("x-xss-protection", "0")
      .add("Content-Length", "100")
  }

  @Benchmark
  def iterate(b: Blackhole): Unit = randommap.foreach(h => b.consume(h))

  @Benchmark
  def keySet(): scala.collection.Set[String] = randommap.keySet

  @Benchmark
  def iterateKeys(b: Blackhole): Unit = doIterateKeys(b, randommap)

  @Benchmark
  def iterateRepeatedKeys(b: Blackhole): Unit = doIterateKeys(b, duplicateKeyMap)

  @Benchmark
  def iterateValues(b: Blackhole): Unit =
    randommap.valuesIterator.foreach(k => b.consume(k))

  private def doIterateKeys(b: Blackhole, map: HeaderMap): Unit =
    map.keysIterator.foreach(k => b.consume(k))
}

class JTMapBackedHeaderMapBenchmark extends HeaderMapBenchmark {
  protected def newMap(): HeaderMap = JTreeMapBackedHeaderMap()
}
