package com.twitter.finagle.thrift

import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.scrooge.TReusableMemoryTransport
import org.apache.thrift.TByteArrayOutputStream
import org.apache.thrift.protocol.{TBinaryProtocol, TProtocol}
import org.openjdk.jmh.annotations._
import scala.util.Random

@State(Scope.Benchmark)
class ProtocolsBenchmark extends StdBenchAnnotations {

  private val LooongStrings: Array[String] = {
    val rnd = new Random(514291442)
    Array(200, 500, 1000, 2000).map(rnd.nextString)
  }

  private[this] val ShortStrings = Array(
    "abcde",
    "verify",
    "increment",
    "colin kaepernick",
    "get_by_screen_name",
    "post_tweet",
    "get_client_application_by_partner_application_id"
  )

  private[this] val MultiByteStrings = Array(
    "\u27F0\u27F4\u27F2\u293C",
    "\u2740\u2741\u2742\u2743\u2744\u2745\u2746\u2747\u2748\u2749",
    "\u270A\u270B\u270C\u270D\u270E\u270F"
  )

  private[this] val ttransport =
    new TReusableMemoryTransport(new TByteArrayOutputStream(10000))

  private[this] var baselineProtocol: TProtocol = _
  private[this] var protocolsProtocol: TProtocol = _

  @Setup(Level.Iteration)
  def setup(): Unit = {
    baselineProtocol = new TBinaryProtocol(ttransport)
    protocolsProtocol = Protocols
      .binaryFactory(statsReceiver = NullStatsReceiver)
      .getProtocol(ttransport)
  }

  private[this] def writeStrings(ss: Array[String], tprotocol: TProtocol): Int = {
    ttransport.reset()
    var i = 0
    while (i < ss.length) {
      tprotocol.writeString(ss(i))
      i += 1
    }
    i
  }

  @Benchmark
  def writeStringShortBaseline(): Int =
    writeStrings(ShortStrings, baselineProtocol)

  @Benchmark
  def writeStringLongBaseline(): Int =
    writeStrings(LooongStrings, baselineProtocol)

  @Benchmark
  def writeStringMultiByteBaseline(): Int =
    writeStrings(MultiByteStrings, baselineProtocol)

  @Benchmark
  def writeStringShortProtocols(): Int =
    writeStrings(ShortStrings, protocolsProtocol)

  @Benchmark
  def writeStringLongProtocols(): Int =
    writeStrings(LooongStrings, protocolsProtocol)

  @Benchmark
  def writeStringMultiByteProtocols(): Int =
    writeStrings(MultiByteStrings, protocolsProtocol)

}
