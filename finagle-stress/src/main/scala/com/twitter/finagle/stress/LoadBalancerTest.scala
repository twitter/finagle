package com.twitter.finagle.integration

import java.util.concurrent.atomic.AtomicInteger

import org.jboss.netty.handler.codec.http._

import com.twitter.ostrich.{StatsCollection, StatsProvider}
import com.twitter.util.{Duration, CountDownLatch, Return, Throw, Time}
import com.twitter.conversions.time._

import com.twitter.finagle.builder.{ClientBuilder, Http}
import com.twitter.finagle.service.Service

object LoadBalancerTest {
  def main(args: Array[String]) {
    runSuite(
      ClientBuilder()
        .requestTimeout(10.milliseconds)
    )

    // TODO: proper resource releasing, etc.
  }

  def runSuite(clientBuilder: ClientBuilder) {
    val baseline = new LoadBalancerTest(clientBuilder)({ case _ => })
    println("** BASELINE")
    baseline.run()
  }

}

class LoadBalancerTest(
  clientBuilder: ClientBuilder,
  serverLatency: Duration = 0.seconds,
  numRequests: Int = 100000,
  concurrency: Int = 50)(behavior: PartialFunction[(Int, Seq[EmbeddedServer]), Unit])
{
  private[this] val requestNumber = new AtomicInteger(0)
  private[this] val requestCount  = new AtomicInteger(numRequests)
  private[this] val latch         = new CountDownLatch(concurrency)
  private[this] val stats         = new StatsCollection

  private[this] def prettyPrintStats(stats: StatsProvider) {
    stats.getCounterStats foreach { case (name, count) =>
      println("# %-30s %d".format(name, count))
    }
  }

  private[this] def dispatch(
      client: Service[HttpRequest, HttpResponse],
      servers: Seq[EmbeddedServer],
      f: PartialFunction[(Int, Seq[EmbeddedServer]), Unit]) {
    val num = requestNumber.incrementAndGet()
    if (f.isDefinedAt((num, servers)))
      f((num, servers))

    client(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")) respond { result =>
      result match {
        case Return(_) =>
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

    val client = clientBuilder
      .codec(Http)
      .hosts(servers map(_.addr))
      .buildService[HttpRequest, HttpResponse]

    val begin = Time.now
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

    servers.zipWithIndex foreach { case (server, which) =>
      server.stop()
      println("> SERVER[%d]".format(which))
      prettyPrintStats(server.stats)
    }
  }
}
