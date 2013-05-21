package com.twitter.finagle.stress

import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.http.Http
import com.twitter.finagle.stats.OstrichStatsReceiver
import com.twitter.finagle.util.DefaultTimer
import com.twitter.ostrich.stats
import com.twitter.util.{Future, Return, Throw, Stopwatch}
import java.net.{SocketAddress, InetSocketAddress}
import java.util.concurrent.CountDownLatch
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http.{
  DefaultHttpRequest, HttpVersion, HttpResponseStatus, HttpMethod, HttpHeaders, HttpRequest
  , HttpResponse, DefaultHttpResponse}

object EndToEndStress {
  private[this] object HttpService
    extends Service[HttpRequest, HttpResponse]
  {
    def apply(request: HttpRequest) = Future {
      val response = new DefaultHttpResponse(
        request.getProtocolVersion, HttpResponseStatus.OK)
      response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, 1)
      response.setContent(ChannelBuffers.wrappedBuffer(".".getBytes))
      response
    }
  }

  @volatile private[this] var running = true

  def dispatchLoop(service: Service[HttpRequest, HttpResponse], latch: CountDownLatch) {
    if (!running) {
      latch.countDown()
      return
    }

    val request = new DefaultHttpRequest(
      HttpVersion.HTTP_1_1, HttpMethod.GET, "/")

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
    val client = buildClient(concurrency, server.localAddress)

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
