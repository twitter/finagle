package com.twitter.finagle.stress

import com.twitter.app.App
import com.twitter.conversions.time._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.Http
import com.twitter.finagle.Service
import com.twitter.finagle.stats.OstrichStatsReceiver
import com.twitter.ostrich.stats.{Stats => OstrichStats}
import com.twitter.ostrich.stats.StatsCollection
import com.twitter.util.{Duration, CountDownLatch, Return, Throw, Stopwatch}

import com.google.caliper.{Param, SimpleBenchmark}
import java.util.concurrent.atomic.AtomicInteger
import org.jboss.netty.handler.codec.http._

import scala.collection.mutable.ArrayBuffer

object LoadBalancerTest extends App {
  val nreqsFlag = flag("n", 100000, "Number of reqs sent from each client")
  val latencyFlag = flag("l", 0.seconds, "req latency forced at the server")
  val tpschedFlag = flag("tpsched", false, "use threadpool scheduler")

  val totalRequests = new AtomicInteger(0)
  val clientBuilder = ClientBuilder()
    .requestTimeout(100.milliseconds)
    .retries(10)

  def main() {
    if (tpschedFlag()) {
      import com.twitter.concurrent.{Scheduler, ThreadPoolScheduler}
      println("Using threadpool scheduler")
      Scheduler.setUnsafe(new ThreadPoolScheduler("FINAGLE"))
    }
    runSuite()
  }

  def doTest(latency: Duration, nreqs: Int,
             behavior: PartialFunction[(Int, Seq[EmbeddedServer]), Unit]) {
      new LoadBalancerTest(clientBuilder, latency, nreqs)(behavior).run()
  }

  def runSuite() {
    val latency = latencyFlag()
    val n = nreqsFlag()
    val N = 10*n

    println("testing " + clientBuilder)
    println("\n== baseline (warmup) ==\n")
    doTest(latency, N, { case _ => })

    println("\n== baseline ==\n")
    doTest(latency, N, { case _ => })

    println("\n== 1 server goes offline ==\n")
    doTest(latency, N, { case (`n`, servers) => servers(1).stop() })

    println("\n== 1 application becomes nonresponsive ==\n")
    doTest(latency, N,
      { case (`n`, servers) => servers(1).becomeApplicationNonresponsive() })

    println("\n== 1 connection becomes nonresponsive ==\n")
    doTest(latency, N,
      { case (`n`, servers) => servers(1).becomeConnectionNonresponsive() })

    println("\n== 1 server has a protocol error ==\n")
    doTest(latency, N,
      { case (`n`, servers) => servers(1).becomeBelligerent() })
  }
}


class LoadBalancerBenchmark extends SimpleBenchmark {
  @Param(Array("0")) val latencyInMilliSec: Long = 0
  @Param(Array("10000")) val nreqs: Int = 10000

  def timeBaseline(reps: Int) {
    var i = 0
    while (i < reps) {
      LoadBalancerTest.doTest(
        Duration.fromMilliseconds(latencyInMilliSec),
        nreqs,
        { case _ => })
      i += 1
    }
  }

  def timeOneOffline(reps: Int) {
    var i = 0
    while (i < reps) {
      LoadBalancerTest.doTest(
        Duration.fromMilliseconds(latencyInMilliSec),
        nreqs,
        { case (n, servers) => servers(1).stop() })
      i += 1
    }
  }

  def timeOneAppNonResponsive(reps: Int) {
    var i = 0
    while (i < reps) {
      LoadBalancerTest.doTest(
        Duration.fromMilliseconds(latencyInMilliSec),
        nreqs,
        { case (n, servers) if n == nreqs/10 =>  servers(1).becomeApplicationNonresponsive() })
      i += 1
    }
  }

  def timeOneConnNonResponsive(reps: Int) {
    var i = 0
    while (i < reps) {
      LoadBalancerTest.doTest(
        Duration.fromMilliseconds(latencyInMilliSec),
        nreqs,
        { case (n, servers) if n == nreqs/10  =>  servers(1).becomeConnectionNonresponsive() })
      i += 1
    }
  }

  def timeOneProtocolError(reps: Int) {
    var i = 0
    while (i < reps) {
      LoadBalancerTest.doTest(
      Duration.fromMilliseconds(latencyInMilliSec),
      nreqs,
      { case (n, servers) if n == nreqs/10  =>  servers(1).becomeBelligerent() })
      i += 1
    }
  }
}

class LoadBalancerTest(
  clientBuilder: ClientBuilder[_, _, _, _, _],
  serverLatency: Duration = 0.seconds,
  numRequests: Int = 100000,
  concurrency: Int = 20)(behavior: PartialFunction[(Int, Seq[EmbeddedServer]), Unit])
{
  private[this] val requestNumber = new AtomicInteger(0)
  private[this] val requestCount  = new AtomicInteger(numRequests)
  private[this] val latch         = new CountDownLatch(concurrency)
  private[this] val stats         = new StatsCollection
  private[this] val gaugeValues   = new ArrayBuffer[(Int, Map[String, Float])]

  private[this] def dispatch(
      client: Service[HttpRequest, HttpResponse],
      servers: Seq[EmbeddedServer],
      f: PartialFunction[(Int, Seq[EmbeddedServer]), Unit]) {
    val num = requestNumber.incrementAndGet()
    LoadBalancerTest.totalRequests.incrementAndGet()
    if (f.isDefinedAt((num, servers)))
      f((num, servers))

    val elapsed = Stopwatch.start()

    client(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")) respond { result =>
      result match {
        case Return(_) =>
          val duration = elapsed()
          stats.addMetric("request_msec", duration.inMilliseconds.toInt)
          stats.addMetric("request_usec", duration.inMicroseconds.toInt)
          stats.incr("success")
        case Throw(exc) =>
          stats.incr("fail")
          stats.incr("fail_%s".format(exc.getClass.getName.split('.').last))
      }

      if (requestCount.decrementAndGet() > 0)
        dispatch(client, servers, f)
      else
        latch.countDown()
    }
  }

  def run() {
    OstrichStats.clearAll()

    val servers = (0 until 3) map { _ =>
      val server = EmbeddedServer()
      server.setLatency(serverLatency)
      server
    }

    val client = clientBuilder
      .codec(Http())
      .hosts(servers map(_.addr))
      .hostConnectionLimit(Int.MaxValue)
      .reportTo(new OstrichStatsReceiver)
      .build()

    val elapsed = Stopwatch.start()
    0 until concurrency foreach { _ => dispatch(client, servers, behavior) }
    latch.await()
    val duration = elapsed()
    val rps = numRequests.toDouble / duration.inMilliseconds.toDouble * 1000

    // Produce a "report" here instead, so we have some sort of
    // semantic information here.

    println("> STATS")
    println("> rps: %.2f".format(rps))
    val succ = stats.getCounter("success")().toDouble
    val fail = stats.getCounter("fail")().toDouble
    println("> success rate: %.2f".format(100.0 * succ / (succ + fail)))
    println("> request rate: %.2f".format(rps))
    Stats.prettyPrint(stats)

    val allGaugeNames = {
      val unique = Set() ++ gaugeValues flatMap { case (_, gvs) => gvs map (_._1) }
      unique.toList.sorted
    }

    println("> %5s %s".format("time", allGaugeNames map("%-8s".format(_)) mkString(" ")))

    gaugeValues foreach { case (requestNum, values) =>
      val columns = allGaugeNames map { name =>
        val value = values.get(name)
        val formatted = value.map("%.2e".format(_)).getOrElse("n/a")
        formatted
      }
      println("> %05d %s".format(requestNum, columns.map("%8s".format(_)).mkString(" ")))
    }

    servers.zipWithIndex foreach { case (server, which) =>
      server.stop()
      println("> SERVER[%d] (%s)".format(which, server.addr))
      Stats.prettyPrint(server.stats)
    }

    println("> OSTRICH counters")
    Stats.prettyPrint(OstrichStats)

    client.close()
    servers foreach { _.stop() }
  }
}
