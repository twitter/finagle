package com.twitter.finagle.stress

import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.httpx.{Fields, Http, Request, Response, Status}
import com.twitter.finagle.stats.OstrichStatsReceiver
import com.twitter.finagle.util.DefaultTimer
import com.twitter.ostrich.stats
import com.twitter.util.{Future, Return, Throw, Stopwatch}
import java.net.{SocketAddress, InetSocketAddress}
import java.util.concurrent.CountDownLatch

object EndToEndStress {
  private[this] object HttpService
    extends Service[Request, Response]
  {
    def apply(request: Request) = Future {
      val response = Response(
        request.version, Status.Ok)
      response.headerMap.set(Fields.ContentLength, "1")
      response.contentString = "."
      response
    }
  }

  @volatile private[this] var running = true

  def dispatchLoop(service: Service[Request, Response], latch: CountDownLatch) {
    if (!running) {
      latch.countDown()
      return
    }

    val request = Request("/")

    val elapsed = Stopwatch.start()

    service(request) ensure {
      dispatchLoop(service, latch)
    } respond {
      case Return(_) =>
        stats.Stats.addMetric("request_msec", elapsed().inMilliseconds.toInt)
      case Throw(_) =>
        stats.Stats.incr("failure")
    }
  }

  private[this] def buildServer() = ServerBuilder()
    .name("stressServer")
    .bindTo(new InetSocketAddress(0))
    .codec(Http())
    .reportTo(new OstrichStatsReceiver)
    .maxConcurrentRequests(5)
    .build(HttpService)

  private[this] def buildClient(concurrency: Int, addr: SocketAddress) = ClientBuilder()
    .name("stressClient")
    .reportTo(new OstrichStatsReceiver)
    .hosts(Seq(addr))
    .codec(Http())
    .hostConnectionLimit(concurrency)
    .build()

  def main(args: Array[String]) {
    println("Starting EndToEndStress")

    val concurrency = 10
    val latch = new CountDownLatch(concurrency)
    val elapsed = Stopwatch.start()

    val server = buildServer()
    val client = buildClient(concurrency, server.boundAddress)

    DefaultTimer.twitter.schedule(10.seconds) {
      println("@@ %ds".format(elapsed().inSeconds))
      Stats.prettyPrintStats()
    }
    0 until concurrency foreach { _ => dispatchLoop(client, latch) }

    Runtime.getRuntime().addShutdownHook(new Thread {
      override def run() {
        val start = System.currentTimeMillis()
        running = false
        latch.await()
        client.close()
        server.close()
        println("Shutdown took %dms".format(System.currentTimeMillis()-start))
      }
    })
  }
}
