package com.twitter.finagle.loadbalancer.roundrobin

import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.Address
import com.twitter.finagle.NoBrokersAvailableException
import com.twitter.finagle.loadbalancer.EndpointFactory
import com.twitter.finagle.loadbalancer.PanicMode
import com.twitter.util.Activity
import com.twitter.util.Var

trait RoundRobinSuite {
  // number of servers
  val N: Int = 100
  // number of reqs
  val R: Int = 100000
  // tolerated variance
  val variance: Double = 0.0001 * R

  trait RRServiceFactory extends EndpointFactory[Unit, Int] {
    def remake() = {}
    val address = Address.Failed(new Exception)
    def meanLoad: Double
  }

  protected val noBrokers: NoBrokersAvailableException = new NoBrokersAvailableException

  def newBal(
    fs: Var[Vector[RRServiceFactory]],
    sr: StatsReceiver = NullStatsReceiver
  ): RoundRobinBalancer[Unit, Int] = new RoundRobinBalancer(
    Activity(fs.map(Activity.Ok(_))),
    statsReceiver = sr,
    emptyException = noBrokers,
    panicMode = PanicMode.TenPercentUnhealthy
  )

  def assertEven(fs: Vector[RRServiceFactory]): Unit = {
    val ml = fs.head.meanLoad
    for (f <- fs) {
      assert(
        math.abs(f.meanLoad - ml) < variance,
        "ml=%f; f.ml=%f; ε=%f".format(ml, f.meanLoad, variance)
      )
    }
  }
}
