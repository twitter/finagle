package com.twitter.finagle

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{Await, Var}
import org.openjdk.jmh.annotations._

/**
 * This is primarily a test for allocations not performance
 * so it is recommended to run with `-prof gc`.
 */
@State(Scope.Benchmark)
class InetResolverBenchmark extends StdBenchAnnotations {

  @Param(Array("8.8.8.8"))
  var ip = ""

  @Param(Array("api.twitter.com"))
  var hostname = ""

  private[this] val inetResolver =
    InetResolver(new InMemoryStatsReceiver())

  private[this] val timeout = 1.second

  private[this] def notPending(addrs: Var[Addr]): Addr =
    Await.result(addrs.changes.filter(_ != Addr.Pending).toFuture(), timeout)

  @Benchmark
  def bindIp(): Addr =
    notPending(inetResolver.bind(ip))

  @Benchmark
  def bindHostname(): Addr =
    notPending(inetResolver.bind(hostname))

}
