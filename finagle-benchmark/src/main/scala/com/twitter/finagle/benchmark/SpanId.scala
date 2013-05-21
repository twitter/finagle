package com.twitter.finagle.benchmark

import com.google.caliper.SimpleBenchmark
import com.twitter.finagle.tracing.SpanId
import scala.util.Random
import com.twitter.util.RichU64Long

// Run thus: bin/caliper finagle/finagle-benchmark com.twitter.finagle.benchmark.SpanIdBenchmark

class SpanIdBenchmark extends SimpleBenchmark {
  private[this] var ids: Array[SpanId] = _
  private[this] val n = 1024

  override protected def setUp() {
    val rng = new Random(31415926535897932L)
    ids = for (_ <- (0 until n) toArray) yield SpanId(rng.nextLong())
  }


  def timeOldToString(nreps: Int) {
    var i = 0
    while (i < nreps) {
      new RichU64Long(ids(i%n).self).toU64HexString
      i += 1
    }
  }

  def timeToString(nreps: Int) {
    var i = 0
    while (i < nreps) {
      ids(i%n).toString
      i += 1
    }
  }
/*
  def timeCharAdd(nreps: Int) {
    val chars = Array[Char]('a', 'b')
    var i = 0
    while (i < nreps) {
      val b = new StringBuilder(16)
      var j = 0
      while (j < 8) {
        b.append(chars)
        j += 1
      }
      b.toString
      i += 1
    }
  }

  def timeStringAdd(nreps: Int) {
    val chars = "ab"
    var i = 0
    while (i < nreps) {
      val b = new StringBuilder(16)
      var j = 0
      while (j < 8) {
        b.append(chars)
        j += 1
      }
      b.toString
      i += 1
    }
  }

  def timeByteAdd(nreps: Int) {
    val bytes = Array[Byte](97, 98)
    var i = 0
    while (i < nreps) {
      val b = new Array[Byte](16)
      var j = 0
      while (j < 8) {
        b(2*j) = bytes(0)
        b(2*j+1) = bytes(1)
        j += 1
      }
      new String(b)
      i += 1
    }
  }
*/
}
