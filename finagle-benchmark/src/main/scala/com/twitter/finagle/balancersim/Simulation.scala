package com.twitter.finagle.balancersim

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.loadbalancer.Balancers
import com.twitter.finagle.stats.SummarizingStatsReceiver
import com.twitter.finagle.NoBrokersAvailableException
import com.twitter.finagle.Stack
import com.twitter.finagle.param
import com.twitter.util.Activity
import com.twitter.util.Future
import com.twitter.util.Stopwatch
import com.twitter.util.Var

private object Simulation extends com.twitter.app.App {
  val qps = flag("qps", 1250, "Number of queries to send per second")
  val dur = flag("dur", 45.seconds, "Benchmark duration")

  val nBackends = flag("backends", 10, "Number of stable uniform backends")
  val nClients = flag("clients", 1, "Number of clients which uniformly send load")
  val bal = flag("bal", "p2c", "The load balancer used by the clients")

  val coldStartBackend = flag("coldstart", false, "Add a cold starting backend")
  val slowMiddleBackend = flag("slowmiddle", false, "Adds a fast-then-slow-then-fast again backend")
  val temporarilyFailedBackend = flag(
    "temporaryfailure",
    false,
    "Adds an unhealthy backend that temporarily fails after 10 seconds for 15 seconds"
  )
  val permanentlyFailedBackend = flag(
    "permanentfailure",
    false,
    "Adds an unhealthy backend that permanently fails after 10 seconds"
  )

  val showProgress = flag("showprogress", false, "Print stats each second")
  val showSummary = flag("showsummary", true, "Print a stats summary at the end of the test")
  val showLoadDist =
    flag("showloaddist", true, "Print a summary of server load distribution at the end of the test")

  // Exception returned by balancers when they have no members in the set.
  private val noBrokers = new NoBrokersAvailableException

  def main(): Unit = {
    val stats = new SummarizingStatsReceiver

    var serverCount: Int = 0
    val genServerId: () => String = () => {
      serverCount += 1
      serverCount.toString
    }

    // create a latency distribution from a set of recorded ping latencies.
    val url = getClass.getClassLoader.getResource("real_latencies.data")
    val stableLatency = LatencyProfile.fromFile(url)
    val alwaysSucceed = FailureProfile.alwaysSucceed

    val servers = Var(
      Seq
        .tabulate(nBackends()) { _ =>
          val id = genServerId()
          ServerFactory(id, stableLatency, alwaysSucceed, stats.scope(s"srv_${id}"))
        }
        .toSet
    )

    val activityServers = Activity(servers.map { srvs => Activity.Ok(srvs.toVector) })

    var clientCount: Int = 0
    val genClientId: () => String = () => {
      clientCount += 1
      clientCount.toString
    }

    val clients: Seq[Client] = Seq.tabulate(nClients()) { _ =>
      val id = genClientId()
      val sr = stats.scope(s"client_${id}")
      // This gives us a nice entry point to have per-client
      // id configurations. For example, we can test two groups
      // of clients (even and odds) running a separate aperture
      // config.
      val balancer = bal() match {
        case "p2c" =>
          Balancers
            .p2c()
            .newBalancer(
              activityServers,
              noBrokers,
              Stack.Params.empty + param.Stats(sr.scope("p2c"))
            )
        case "ewma" =>
          Balancers
            .p2cPeakEwma()
            .newBalancer(
              activityServers,
              noBrokers,
              Stack.Params.empty + param.Stats(sr.scope("p2c_ewma"))
            )
        case "aperture" =>
          Balancers
            .aperture()
            .newBalancer(
              activityServers,
              noBrokers,
              Stack.Params.empty + param.Stats(sr.scope("aperture"))
            )
        case "rr" =>
          Balancers
            .roundRobin()
            .newBalancer(
              activityServers,
              noBrokers,
              Stack.Params.empty + param.Stats(sr.scope("round_robin"))
            )
      }
      ClientFactory(id, balancer, sr)
    }

    val query: () => Future[Unit] = () => {
      Future
        .collect(clients.map { clnt => clnt(()) })
        .unit
    }

    val elapsed = Stopwatch.start()
    val qpms = qps() / 1000
    val rem = qps() % 1000
    var ms = 0

    val p = new LatencyProfile(elapsed)
    val f = new FailureProfile(elapsed)

    // TODO: These latency events are dependent on the running time of
    // the simulation. They should probably be defined in terms of a ratio
    // of the running time to be more flexible.
    if (coldStartBackend()) {
      val coldStart = p.warmup(10.seconds) _ andThen p.slowWithin(19.seconds, 23.seconds, 10)
      servers() += ServerFactory(
        genServerId(),
        coldStart(stableLatency),
        alwaysSucceed,
        stats.scope("srv_cold_start")
      )
    }

    if (slowMiddleBackend()) {
      val slowMiddle = p.slowWithin(15.seconds, 45.seconds, 10) _
      servers() += ServerFactory(
        genServerId(),
        slowMiddle(stableLatency),
        alwaysSucceed,
        stats.scope("srv_slow_middle")
      )
    }

    if (temporarilyFailedBackend()) {
      val failWithin = f.failWithin(10.seconds, 25.seconds)
      servers() += ServerFactory(
        genServerId(),
        stableLatency,
        failWithin,
        stats.scope("srv_unhealthy_temporarily")
      )
    }

    if (permanentlyFailedBackend()) {
      val failAfter = f.failAfter(10.seconds)
      servers() += ServerFactory(
        genServerId(),
        stableLatency,
        failAfter,
        stats.scope("srv_unhealthy_permanently")
      )
    }

    // Note, it's important to actually elapse time here instead of
    // synthesizing it since we want to see the effects of latency
    // on the load balancer.
    while (elapsed() < dur()) {
      Thread.sleep(1)

      var n = 0
      while (n < qpms) {
        query()
        n += 1
      }

      if (rem > 0 && ms % (1000 / rem) == 0) { query() }

      ms += 1

      if (showProgress() & ms % 1000 == 0) {
        println("-" * 100)
        println(s"Requests at ${elapsed()}")

        val lines =
          for ((name, fn) <- stats.gauges.toSeq)
            yield (name.mkString("/"), fn())
        for ((name, value) <- lines.sortBy(_._1))
          println(s"$name $value")
      }
    }

    if (showSummary()) {
      println(stats.summary(includeTails = true))
    }

    if (showLoadDist()) {
      val srvs = servers.sample().toSeq
      val totalLoad = srvs.map(_.count).sum
      val optimal = totalLoad / serverCount.toDouble
      println("# load distribution")
      println(f"optimal (total / servers): ${optimal}%1.2f")
      srvs.sortBy(_.count).foreach { srv =>
        val variance = math.abs(srv.count - optimal)
        val variancePct = (variance / optimal.toDouble) * 100
        val successRate = srv.successRate
        println(
          s"srv=${srv.toString} load=${srv.count} " +
            f"variance=$variance%1.2f (${variancePct}%1.2f%%) " +
            f"successRate=$successRate%1.2f"
        )
      }
      // TODO: export standard deviation.
    }
  }
}
