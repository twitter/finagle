package com.twitter.finagle.benchmark

import com.google.caliper.SimpleBenchmark
import scala.util.Random
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import com.google.common.base.Charsets
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.memcached.util.ParserUtils

// From $BIRDCAGE_HOME run:
// ./pants bench finagle/finagle-benchmark: --bench-target=com.twitter.finagle.benchmark.ParserUtils --bench-memory
class ParserUtils extends SimpleBenchmark {

  private val size = 100000
  private val rnd = new Random(69230L) // just to give us consistent values
  private val numbers: Seq[ChannelBuffer] = Seq.fill(size) {
    ChannelBuffers.copiedBuffer(rnd.nextInt().toString, Charsets.UTF_8)
  }
  private val strings: Seq[ChannelBuffer] = Seq.fill(size) {
    ChannelBuffers.copiedBuffer(rnd.nextString(5), Charsets.UTF_8)
  }

  def matches(buffer: ChannelBuffer, string: String): Boolean =
    buffer.toString(Charsets.UTF_8).matches(string)

  def timeDIGITS(nreps: Int) {
    var i = 0
    while (i < nreps) {
      numbers.foreach(matches(_, ParserUtils.DIGITS))
      strings foreach(matches(_, ParserUtils.DIGITS))
      i += 1
    }
  }

  def timeIsDigits(nreps: Int) {
    var i = 0
    while (i < nreps) {
      numbers foreach { cb =>
        ParserUtils.isDigits(cb)
      }
      strings foreach { cb =>
        ParserUtils.isDigits(cb)
      }
      i += 1
    }
  }

}
