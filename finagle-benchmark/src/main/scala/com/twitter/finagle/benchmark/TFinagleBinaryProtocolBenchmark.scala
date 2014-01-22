package com.twitter.finagle.benchmark

import com.google.caliper.SimpleBenchmark
import com.twitter.finagle.thrift.Protocols
import org.apache.thrift.protocol.{TProtocol, TBinaryProtocol}
import org.apache.thrift.transport.TMemoryBuffer
import scala.util.Random

// ./pants goal bench finagle/finagle-benchmark --bench-target=com.twitter.finagle.benchmark.TFinagleBinaryProtocolBenchmark
class TFinagleBinaryProtocolBenchmark extends SimpleBenchmark {

  private val LooongStrings: Seq[String] = {
    val rnd = new Random(514291442)
    Seq(200, 500, 1000, 2000) map { rnd.nextString(_) }
  }

  private val Strings = Seq(
    "abcde",
    "verify",
    "increment",
    "colin kaepernick",
    "get_by_screen_name",
    "post_tweet",
    "get_client_application_by_partner_application_id"
  )

  private val MultiByteStrings = Seq(
    "\u27F0\u27F4\u27F2\u293C",
    "\u2740\u2741\u2742\u2743\u2744\u2745\u2746\u2747\u2748\u2749",
    "\u270A\u270B\u270C\u270D\u270E\u270F"
  )

  private def newTransport = new TMemoryBuffer(5000000)

  private def testProtocol(nreps: Int, strings: Seq[String], proto: TProtocol) {
    var i = 0
    while (i < nreps) {
      var j = 0
      while (j < strings.length) {
        proto.writeString(strings(j))
        j += 1
      }
      i += 1
    }
  }

  def timeTBinaryProtocolWriteString(nreps: Int) {
    val proto = new TBinaryProtocol(newTransport)
    testProtocol(nreps, Strings, proto)
  }

  def timeTFinagleBinaryProtocolWriteString(nreps: Int) {
    val proto = Protocols.binaryFactory().getProtocol(newTransport)
    testProtocol(nreps, Strings, proto)
  }

  def timeTBinaryProtocolWriteStringLong(nreps: Int) {
    val proto = new TBinaryProtocol(newTransport)
    testProtocol(nreps, LooongStrings, proto)
  }

  def timeTFinagleBinaryProtocolWriteStringLong(nreps: Int) {
    val proto = Protocols.binaryFactory().getProtocol(newTransport)
    testProtocol(nreps, LooongStrings, proto)
  }

  def timeTBinaryProtocolWriteStringMultiByte(nreps: Int) {
    val proto = new TBinaryProtocol(newTransport)
    testProtocol(nreps, MultiByteStrings, proto)
  }

  def timeTFinagleBinaryProtocolWriteStringMultiByte(nreps: Int) {
    val proto = Protocols.binaryFactory().getProtocol(newTransport)
    testProtocol(nreps, MultiByteStrings, proto)
  }

  def timeTBinaryProtocolWriteStringEmptyString(nreps: Int) {
    val proto = new TBinaryProtocol(newTransport)
    testProtocol(nreps, Seq(""), proto)
  }

  def timeTFinagleBinaryProtocolWriteStringEmptyString(nreps: Int) {
    val proto = Protocols.binaryFactory().getProtocol(newTransport)
    testProtocol(nreps, Seq(""), proto)
  }

}
