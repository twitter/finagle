package com.twitter.finagle.topo

import com.twitter.finagle.Service
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.Http
import com.twitter.finagle.stats.OstrichStatsReceiver
import com.twitter.logging.{Level, LoggerFactory, ConsoleHandler}
import com.twitter.ostrich.admin.{RuntimeEnvironment, AdminHttpService}
import org.jboss.netty.handler.codec.http._

object Client {
  private[this] def go(svc: Service[HttpRequest, HttpResponse]) {
    val req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
    svc(req) ensure { go(svc) }
  }

  def main(args: Array[String]) {
    LoggerFactory(
      node = "",
      level = Some(Level.INFO),
      handlers = ConsoleHandler() :: Nil
    ).apply()

    if (args.size != 3) {
      System.err.printf("usage: Client statsport host:port concurrency\n")
      System.exit(1)
    }

    val statsPort = args(0).toInt
    val hostport = args(1)
    val n = args(2).toInt

    val runtime = RuntimeEnvironment(this, Array()/*no args for you*/)
    val adminService = new AdminHttpService(statsPort, 100/*backlog*/, runtime)
    adminService.start()
    
    val service = Http.newService(hostport)

    for (which <- 0 until n)
      go(service)
  }
}
