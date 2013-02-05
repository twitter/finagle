package com.twitter.finagle.stress

import com.twitter.finagle.stats.OstrichStatsReceiver
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.channel.OpenConnectionsThresholds
import com.twitter.finagle.Service
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._
import java.net.{SocketAddress, InetSocketAddress}
import java.util.concurrent.CountDownLatch
import com.twitter.finagle.http.Http
import com.twitter.util._
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.conversions.time._
import com.twitter.ostrich.stats

object FrontEndServerStress {

  object DummyService
    extends Service[HttpRequest, HttpResponse]
  {
    def apply(request: HttpRequest) = Future {
      val response = new DefaultHttpResponse(
        request.getProtocolVersion, HttpResponseStatus.OK)
      response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, 1)
      val content = List.range(1,1000).map(_.toByte).toArray
      response.setContent(ChannelBuffers.wrappedBuffer(content))
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
    val client = buildClient(concurrency, server.localAddress)

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
