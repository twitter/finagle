package com.twitter.finagle.benchmark

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.loadbalancer.P2CBalancer
import com.twitter.finagle.stats.{StatsReceiver, SummarizingStatsReceiver}
import com.twitter.finagle.util.{Drv, Rng, DefaultTimer}
import com.twitter.util.{Function => _, _}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.TimeUnit
import scala.io.Source

private[finagle] object LatencyProfile {
  private val rng = Rng("seed".hashCode)
  /**
   * Creates a latency profile from a file where each line
   * represents recorded latencies.
   */
  def fromFile(path: java.net.URL): () => Duration = {
    val latencies = Source.fromURL(path).getLines.toIndexedSeq map {
      line: String => Duration.fromNanoseconds((line.toDouble*1000000).toLong)
    }
    val size = latencies.size
    var i = rng.nextInt(size)
    () => { i = i + 1; latencies(i%size) }
  }

  def apply(
    dist: IndexedSeq[Double],
    latencies: IndexedSeq[Duration]
  ): () => Duration = {
    val drv = Drv(dist)
    () => latencies(drv(rng))
  }
}

private[finagle] class LatencyProfile(stopWatch: () => Duration) {
  /** Increase latency returned from `next` by `factor`. */
  def slowBy(factor: Long)(next: () => Duration) = () => { next()*factor }

  /**
   * Increases the latency returned from `next` by `factor` while `stopWatch` is
   * within `start` and `end`.
   */
  def slowWithin(start: Duration, end: Duration, factor: Long)(next: () => Duration) = () => {
    val time = stopWatch()
    if (time >= start && time <= end) next()*factor else next()
  }

  /**
   * Progressively improve latencies returned from `next` while `stopWatch` is still
   * within the window terminated at `end`.
   */
  def warmup(end: Duration, maxFactor: Double = 5.0)(next: () => Duration) = () => {
    val time = stopWatch()
    val factor = if (time < end) (1.0/time.inNanoseconds)*(end.inNanoseconds) else 1.0
    Duration.fromNanoseconds((next().inNanoseconds*factor.min(maxFactor)).toLong)
  }
}

/**
 * Creates a ServiceFactory that applies a latency profile to Services
 * it creates.
 */
private[finagle] class LatencyFactory(sr: StatsReceiver) {
  def apply(name: Int, next: () => Duration): ServiceFactory[Unit, Unit] = {
    val service = new Service[Unit, Unit] {
      implicit val timer = DefaultTimer.twitter
      val load = new AtomicInteger(0)
      val maxload = new AtomicInteger(0)
      val gauges = Seq(
        sr.scope("load").addGauge(""+name) { load.get() },
        sr.scope("maxload").addGauge(""+name) { maxload.get() }
      )
      val count = sr.scope("count").counter(""+name)

      def apply(req: Unit) = {
        synchronized {
          val l = load.incrementAndGet()
          if (l > maxload.get()) maxload.set(l)
        }
        Future.sleep(next()) ensure {
          count.incr()
          load.decrementAndGet()
        }
      }
    }

    new ServiceFactory[Unit, Unit] {
      def apply(conn: ClientConnection) = Future.value(service)
      def close(deadline: Time) = Future.Done
      override def toString = name.toString
    }
  }
}

private[finagle] object P2CBenchmark extends com.twitter.app.App {
  val qps = flag("qps", 1250, "QPS at which to run the benchmark")
  val dur = flag("dur", 45.seconds, "Benchmark duration")
  def main() {
    val Qpms = qps()/1000
    val Rem = qps()%1000

    val stats = new SummarizingStatsReceiver
    val newFactory = new LatencyFactory(stats)

    val data = getClass.getClassLoader.getResource("resources/real_latencies.data")
    val dist = LatencyProfile.fromFile(data)
    val stable: Seq[ServiceFactory[Unit, Unit]] = Seq.tabulate(9) { i => newFactory(i, dist) }

    val underlying = Var(stable)
    val p2c = new P2CBalancer[Unit, Unit](
      Activity(underlying map { facs => Activity.Ok(facs map { fac => (fac, 1D) }) }),
      statsReceiver=stats.scope("p2c"))
    val balancer = p2c.toService

    val latstat = stats.stat("latency")
    def call() = stats.timeFuture(TimeUnit.MILLISECONDS, latstat) { balancer(()) }

    val stopWatch = Stopwatch.start()
    val p = new LatencyProfile(stopWatch)

    val coldStart = p.warmup(10.seconds)_ andThen p.slowWithin(19.seconds, 23.seconds, 10)
    underlying() :+= newFactory(10, coldStart(dist))
    underlying() :+= newFactory(11, p.slowBy(2)(dist))

    var ms = 0
    while (stopWatch() < dur()) {
      Thread.sleep(1)

      var n = 0
      while (n < Qpms) {
        call()
        n += 1
      }

      if (Rem > 0 && ms%(1000/Rem) == 0) { call() }

      ms += 1

      if (ms%1000==0) {
        println("-"*100)
        println("Requests at %s".format(stopWatch()))

        val lines = for ((name, fn) <- stats.gauges.toSeq) yield (name.mkString("/"), fn())
        for ((name, value) <- lines.sortBy(_._1))
          println(name+" "+value)
      }
    }

    println(stats.summary(includeTails = true))
  }
}
