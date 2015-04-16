package com.twitter.finagle.util

import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.core.util.InetAddressUtil
import java.net.InetAddress
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
@Threads(1)
class InetAddressUtilBench extends StdBenchAnnotations {
  val ip = "69.55.236.117"

  @Benchmark
  def timeOldInetAddressGetByName(): InetAddress = {
    InetAddress.getByName(ip)
  }

  @Benchmark
  def timeNewInetAddressGetByName(): InetAddress = {
    InetAddressUtil.getByName(ip)
  }
}
