package com.twitter.finagle.stress

import com.twitter.finagle.stats.OstrichStatsReceiver
import com.twitter.finagle.util.Timer
import com.twitter.finagle.channel.OpenConnectionsThresholds
import com.twitter.finagle.Service
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._
import java.net.InetSocketAddress
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

  def dispatchLoop(service: Service[HttpRequest, HttpResponse]) {
    val request = new DefaultHttpRequest(
      HttpVersion.HTTP_1_1, HttpMethod.GET, "/")

    val beginTime = Time.now

    service(request) ensure {
      dispatchLoop(service)
    } respond {
      case Return(_) =>
        stats.Stats.addMetric("request_msec", beginTime.untilNow.inMilliseconds.toInt)
      case Throw(_) =>
        stats.Stats.incr("failure")
    }
  }

  private[this] def run(concurrency: Int, addr: InetSocketAddress) {
    val service = ClientBuilder()
      .name("stressClient")
      .reportTo(new OstrichStatsReceiver)
      .hosts(Seq(addr))
      .codec(Http())
      .hostConnectionLimit(1500)
      .build()

    0 until concurrency foreach { _ => dispatchLoop(service) }
  }


  def main( args : Array[String] ) {
    val serverAddr = RandomSocket()
    val server = ServerBuilder()
      .name("stressServer")
      .bindTo(serverAddr)
      .codec(Http())
      .reportTo(new OstrichStatsReceiver)
      .openConnectionsThresholds( OpenConnectionsThresholds(500, 1000, 30.seconds) )
      .build(DummyService)

    val beginTime = Time.now
    Timer.default.schedule(10.seconds) {
      println("@@ %ds".format(beginTime.untilNow.inSeconds))
      Stats.prettyPrintStats()
    }

    run(1500, serverAddr)
  }
}
