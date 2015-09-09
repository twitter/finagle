package com.twitter.finagle.stress

import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.httpx.{Fields, Http, Request, Response, Status}
import com.twitter.finagle.netty3.channel.OpenConnectionsThresholds
import com.twitter.finagle.stats.OstrichStatsReceiver
import com.twitter.finagle.util.DefaultTimer
import com.twitter.io.Buf
import com.twitter.ostrich.stats
import com.twitter.util._
import java.net.{InetSocketAddress, SocketAddress}
import java.util.concurrent.CountDownLatch

object FrontEndServerStress {

  object DummyService
    extends Service[Request, Response]
  {
    def apply(request: Request) = Future {
      val response = Response(
        request.version, Status.Ok)
      response.headerMap.set(Fields.ContentLength, "1")
      val content = List.range(1,1000).map(_.toByte).toArray
      response.content = Buf.ByteArray.Owned(content)
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

    val beginTime = Time.now

    service(request) ensure {
      dispatchLoop(service, latch)
    } respond {
      case Return(_) =>
        stats.Stats.addMetric("request_msec", beginTime.untilNow.inMilliseconds.toInt)
      case Throw(_) =>
        stats.Stats.incr("failure")
    }
  }

  private[this] def buildServer() = ServerBuilder()
    .name("stressServer")
    .bindTo(new InetSocketAddress(0))
    .codec(Http())
    .reportTo(new OstrichStatsReceiver)
    .openConnectionsThresholds( OpenConnectionsThresholds(500, 1000, 30.seconds) )
    .build(DummyService)

  private[this] def buildClient(concurrency: Int, addr: SocketAddress) = ClientBuilder()
    .name("stressClient")
    .reportTo(new OstrichStatsReceiver)
    .hosts(Seq(addr))
    .codec(Http())
    .hostConnectionLimit(concurrency)
    .build()

  def main( args : Array[String] ) {
    println("Starting FrontEndServerStress")
    val concurrency = 1500
    val latch = new CountDownLatch(concurrency)
    val beginTime = Time.now

    val server = buildServer()
    val client = buildClient(concurrency, server.boundAddress)

    DefaultTimer.twitter.schedule(10.seconds) {
      println("@@ %ds".format(beginTime.untilNow.inSeconds))
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
