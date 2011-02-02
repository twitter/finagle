package com.twitter.finagle.stress

import scala.collection.mutable.{ArrayBuffer, HashMap}

import java.util.concurrent.atomic.AtomicInteger

import org.jboss.netty.handler.codec.http._

import com.twitter.ostrich.{StatsCollection, StatsProvider, Stats}
import com.twitter.util.{Duration, CountDownLatch, Return, Throw, Time}
import com.twitter.conversions.time._

import com.twitter.finagle.builder.{ClientBuilder, Http}
import com.twitter.finagle.Service
import com.twitter.finagle.stats.{NullStatsRepository, OstrichStatsReceiver}
import com.twitter.finagle.util.Timer
import com.twitter.finagle.util.Conversions._

object LoadBalancerTest {
  def main(args: Array[String]) {
    // Make the type enforced by the *codec*

    runSuite(
      ClientBuilder()
        .requestTimeout(100.milliseconds)
        .retries(10)
    )

    // TODO: proper resource releasing, etc.
  }

  def runSuite(clientBuilder: ClientBuilder[_, _]) {
    println("testing " + clientBuilder)
    println("\n== baseline (warmup) ==\n")
    new LoadBalancerTest(clientBuilder)({ case _ => }).run()

    println("\n== baseline ==\n")
    new LoadBalancerTest(clientBuilder)({ case _ => }).run()

    println("\n== 1 server goes offline ==\n")
    new LoadBalancerTest(clientBuilder)({
      case (10000, servers) =>
        servers(1).stop()
    }).run()

    println("\n== 1 application becomes nonresponsive ==\n")
    new LoadBalancerTest(clientBuilder)({
      case (10000, servers) =>
        servers(1).becomeApplicationNonresponsive()
    }).run()

    println("\n== 1 connection becomes nonresponsive ==\n")
    new LoadBalancerTest(clientBuilder)({
      case (10000, servers) =>
        servers(1).becomeConnectionNonresponsive()
    }).run()

    println("\n== 1 server has a protocol error ==\n")
    new LoadBalancerTest(clientBuilder)({
      case (10000, servers) =>
        servers(1).becomeBelligerent()
    }).run()
  }
}

class LoadBalancerTest(
  clientBuilder: ClientBuilder[_, _],
  serverLatency: Duration = 0.seconds,
  numRequests: Int = 100000,
  concurrency: Int = 20)(behavior: PartialFunction[(Int, Seq[EmbeddedServer]), Unit])
{
  private[this] val requestNumber = new AtomicInteger(0)
  private[this] val requestCount  = new AtomicInteger(numRequests)
  private[this] val latch         = new CountDownLatch(concurrency)
  private[this] val stats         = new StatsCollection
  private[this] val gaugeValues   = new ArrayBuffer[(Int, Map[String, Float])]

  private[this] def prettyPrintStats(stats: StatsProvider) {
    stats.getCounterStats foreach { case (name, count) =>
      println("# %-30s %d".format(name, count))
    }

    stats.getTimingStats foreach { case (name, stat) =>
      val statMap = stat.toMap
      val keys = statMap.keys.toList.sorted

      keys foreach { key =>
        println("# %-30s %s".format("request_%s".format(key), statMap(key)))
      }
    }
  }

  private[this] def dispatch(
      client: Service[HttpRequest, HttpResponse],
      servers: Seq[EmbeddedServer],
      f: PartialFunction[(Int, Seq[EmbeddedServer]), Unit]) {
    val num = requestNumber.incrementAndGet()
    if (f.isDefinedAt((num, servers)))
      f((num, servers))

    val beginTime = Time.now

    client(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")) respond { result =>
      result match {
        case Return(_) =>
          val duration = beginTime.untilNow
          stats.addTiming("request", duration.inMilliseconds.toInt)
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
    val servers = (0 until 3).toArray map(_ => EmbeddedServer())

    servers foreach { server =>
      server.setLatency(serverLatency)
    }

    // Capture gauges to report them at the end.
    val gauges = new HashMap[Seq[(String, String)], Function0[Float]]
    val localStatsReceiver = new NullStatsRepository {
      override def mkGauge(description: Seq[(String, String)], f: => Float) {
        gauges += description -> (() => f)
      }
    }
    // Also report to the main Ostrich stats object.
    Stats.clearAll()
    val statsReceiver = localStatsReceiver.reportTo(new OstrichStatsReceiver)

    def captureGauges() {
      Timer.default.schedule(500.milliseconds) {
        val now = requestNumber.get
        val values = gauges map { case (description, v) =>
          val options = Map(description: _*)
          val host = options("host")
          val serverIndex = servers.findIndexOf { _.addr.toString == host }
          val shortName = options("name") match {
            case "load" => "l"
            case "weight" => "w"
            case "available" => "a"
            case _ => "u"
          }

          val name = "%d/%s".format(serverIndex, shortName)

          (name, v())
        }
        gaugeValues += ((now, Map() ++ values))
       }
    }

    val client = clientBuilder
      .codec(Http)
      .hosts(servers map(_.addr))
      .reportTo(statsReceiver)
      .build()

    val begin = Time.now
    captureGauges()
    0 until concurrency foreach { _ => dispatch(client, servers, behavior) }
    latch.await()
    val duration = begin.untilNow
    val rps = (numRequests.toDouble / duration.inMilliseconds.toDouble) * 1000.0

    // Produce a "report" here instead, so we have some sort of
    // semantic information here.

    println("> STATS")
    val succ = stats.getCounter("success")().toDouble
    val fail = stats.getCounter("fail")().toDouble
    println("> success rate: %.2f".format(100.0 * succ / (succ + fail)))
    println("> request rate: %.2f".format(rps))
    prettyPrintStats(stats)

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
      prettyPrintStats(server.stats)
    }

    println("> OSTRICH counters")
    prettyPrintStats(Stats)
  }
}
