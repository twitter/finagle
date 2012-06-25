package com.twitter.finagle.benchmark

import com.google.caliper.SimpleBenchmark
import com.twitter.finagle.core.util.InetAddressUtil
import java.net.InetAddress

// bin/caliper finagle/finagle-benchmark com.twitter.finagle.benchmark.InetAddressUtilBenchmark

class InetAddressUtilBenchmark extends SimpleBenchmark {
  val Ip = "69.55.236.117"

  def timeOldInetAddressGetByName(nreps: Int) {
    var i = 0
    while (i < nreps) {
      InetAddress.getByName(Ip)
      i += 1
    }
  }

  def timeNewInetAddressGetByName(nreps: Int) {
    var i = 0
    while (i < nreps) {
      InetAddressUtil.getByName(Ip)
      i += 1
    }
  }
}
