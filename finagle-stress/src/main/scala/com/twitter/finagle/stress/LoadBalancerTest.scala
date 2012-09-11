package com.twitter.finagle.stress

import com.twitter.conversions.time._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.Http
import com.twitter.finagle.Service
import com.twitter.finagle.stats.OstrichStatsReceiver
import com.twitter.ostrich.stats.{Stats => OstrichStats}
import com.twitter.ostrich.stats.StatsCollection
import com.twitter.util.{Duration, CountDownLatch, Return, Throw, Time}

import java.util.concurrent.atomic.AtomicInteger
import org.jboss.netty.handler.codec.http._

import scala.collection.mutable.ArrayBuffer

object LoadBalancerTest {
  val totalRequests = new AtomicInteger(0)

  def main(args: Array[String]) {
    // Make the type enforced by the *codec*

    runSuite(
      ClientBuilder()
        .requestTimeout(100.milliseconds)
        .retries(10)
    )
  }

  def runSuite(clientBuilder: ClientBuilder[_, _, _, _, _]) {
    val n = 10000
    val latency = 0.seconds
    println("testing " + clientBuilder)
    println("\n== baseline (warmup) ==\n")
    new LoadBalancerTest(clientBuilder, latency, 10*n)({ case _ => }).run()

    println("\n== baseline ==\n")
    new LoadBalancerTest(clientBuilder, latency, 10*n)({ case _ => }).run()

    println("\n== 1 server goes offline ==\n")
    new LoadBalancerTest(clientBuilder, latency, 10*n)({
      case (`n`, servers) =>
        servers(1).stop()
    }).run()

    println("\n== 1 application becomes nonresponsive ==\n")
    new LoadBalancerTest(clientBuilder, latency, 10*n)({
      case (`n`, servers) =>
        servers(1).becomeApplicationNonresponsive()
    }).run()

    println("\n== 1 connection becomes nonresponsive ==\n")
    new LoadBalancerTest(clientBuilder, latency, 10*n)({
      case (`n`, servers) =>
        servers(1).becomeConnectionNonresponsive()
    }).run()

    println("\n== 1 server has a protocol error ==\n")
    new LoadBalancerTest(clientBuilder, latency, 10*n)({
      case (`n`, servers) =>
        servers(1).becomeBelligerent()
    }).run()
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

    val beginTime = Time.now

    client(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")) respond { result =>
      result match {
        case Return(_) =>
          val duration = beginTime.untilNow
          stats.addMetric("request_msec", duration.inMilliseconds.toInt)
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

    val begin = Time.now
    0 until concurrency foreach { _ => dispatch(client, servers, behavior) }
    latch.await()
    val duration = begin.untilNow
    val rps = numRequests.toDouble / duration.inSeconds.toDouble

    // Produce a "report" here instead, so we have some sort of
    // semantic information here.

    println("> STATS")
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

    client.release()
    servers foreach { _.stop() }
  }
}
