package com.twitter.finagle.integration

import java.util.concurrent.atomic.AtomicInteger

import org.jboss.netty.handler.codec.http._

import org.specs.Specification

import com.twitter.conversions.time._
import com.twitter.util.{Return, Throw, CountDownLatch, Time}

import com.twitter.ostrich.{StatsCollection, StatsProvider}

import com.twitter.finagle.service.Service
import com.twitter.finagle.builder.{ClientBuilder, Http}

object LoadBalancerIntegrationSpec extends Specification {
  def prettyPrintStats(stats: StatsProvider) {
    stats.getCounterStats foreach { case (name, count) =>
      println("# %-30s %d".format(name, count))
    }
  }

  "Load Balancer" should {
    // def runSuite(client: Service[HttpRequest, HttpResponse])
    val numRequests = 50000
    val concurrency = 50

    val servers = (0 until 3).toArray map(_ => EmbeddedServer())
    val stats = new StatsCollection
    val requestNumber = new AtomicInteger(0)
    val requestCount = new AtomicInteger(numRequests)
    val latch = new CountDownLatch(concurrency)

    servers foreach { server =>
      server.setLatency(5.milliseconds)
    }

    // XXX - periodically print load, etc [or any kind of debugging
    // information from the loadbalancer]

    doAfter {
      servers.zipWithIndex foreach { case (server, which) =>
        server.stop()
        println("> SERVER[%d]".format(which))
        prettyPrintStats(server.stats)
      }
    }

    def dispatch(client: Service[HttpRequest, HttpResponse], f: PartialFunction[Int, Unit]) {
      val num = requestNumber.incrementAndGet()
      if (f.isDefinedAt(num))
        f(num)

      client(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")) respond { result =>
        result match {
          case Return(_) =>
            stats.incr("success")
          case Throw(exc) =>
            stats.incr("fail")
            stats.incr("fail_%s".format(exc.getClass.getName.split('.').last))
        }

        if (requestCount.decrementAndGet() > 0)
          dispatch(client, f)
        else
          latch.countDown()
      }
    }

    def runTest[A](client: Service[HttpRequest, HttpResponse])(f: PartialFunction[Int, Unit]) {
      val begin = Time.now
      0 until concurrency foreach { _ => dispatch(client, f) }
      latch.await()
      val duration = begin.untilNow()
      val rps = (numRequests.toDouble / duration.inMilliseconds.toDouble) * 1000.0
      
      println("> STATS")
      val succ = stats.getCounter("success")().toDouble
      val fail = stats.getCounter("fail")().toDouble
      println("> success rate: %.2f".format(100.0 * succ / (succ + fail)))
      println("> request rate: %.2f".format(rps))
      prettyPrintStats(stats)
    }

    "balance: baseline" in {
      val client = ClientBuilder()
        .codec(Http)
        .hosts(servers map(_.addr))
        .retries(2)
        .requestTimeout(50.milliseconds)
        .buildService[HttpRequest, HttpResponse]

      runTest(client) { case _ => () }

      true must beTrue
    }

    "balance: server goes offline" in {
      val client = ClientBuilder()
        .codec(Http)
        .hosts(servers map(_.addr))
        .retries(2)
        .requestTimeout(50.milliseconds)
        .buildService[HttpRequest, HttpResponse]

      runTest(client) {
        case 100 =>
          servers(1).stop()
      }

      true must beTrue
    }

    "balance: application becomes nonresponsive" in {
      val client = ClientBuilder()
        .codec(Http)
        .hosts(servers map(_.addr))
        .requestTimeout(50.milliseconds)
        // .retries(2)
        .buildService[HttpRequest, HttpResponse]

      runTest(client) {
        case 100 =>
          servers(1).becomeApplicationNonresponsive()
      }

      true must beTrue
    }

    "balance: connection becomes nonresponsive" in {
      val client = ClientBuilder()
        .codec(Http)
        .hosts(servers map(_.addr))
        // .retries(2)
        .requestTimeout(50.milliseconds)
        .buildService[HttpRequest, HttpResponse]

      runTest(client) {
        case 100 =>
          servers(1).becomeConnectionNonresponsive()
      }

      true must beTrue
    }

    "balance: server has protocol error" in {
      val client = ClientBuilder()
        .codec(Http)
        .hosts(servers map(_.addr))
        // .retries(2)
        .requestTimeout(50.milliseconds)
        .buildService[HttpRequest, HttpResponse]

      runTest(client) {
        case 100 =>
          servers(1).becomeBelligerent()
      }

      true must beTrue
    }
  }
}
