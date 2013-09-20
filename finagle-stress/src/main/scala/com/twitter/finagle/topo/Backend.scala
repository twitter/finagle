package com.twitter.finagle.topo

import com.twitter.app.App
import com.twitter.conversions.time._
import com.twitter.finagle.ThriftMux
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.stats.OstrichStatsReceiver
import com.twitter.finagle.thrift.ThriftServerFramedCodec
import com.twitter.logging.Logging
import com.twitter.logging.{Level, Logger, LoggerFactory, ConsoleHandler}
import com.twitter.ostrich.admin.{RuntimeEnvironment, AdminHttpService}
import com.twitter.util.{Future, JavaTimer, Await}
import java.net.InetSocketAddress
import org.apache.thrift.protocol.TBinaryProtocol

object BackendService extends thrift.Backend.FutureIface {
  private[this] val timer = new JavaTimer

  private[this] def makeResponse(size: Int) = if (size==0) "" else "."*size

  def request(size: Int, latencyMs: Int) =
    if (latencyMs <= 0)
      Future.value(makeResponse(size))
    else
      timer.doLater(latencyMs.milliseconds) { makeResponse(size) }
}

object Backendserver extends App with Logging {
  val basePort = flag("baseport", 2000, "Base port")
  val useThriftmux = flag("thriftmux", true, "Use thriftmux")

  def main() {
    val runtime = RuntimeEnvironment(this, Array()/*no args for you*/)
    val adminService = new AdminHttpService(basePort()+1, 100/*backlog*/, runtime)
    adminService.start()
    
    if (useThriftmux())
      Await.ready(ThriftMux.serveIface(":"+basePort(), BackendService))
    else {
      val service = new thrift.Backend.FinagledService(
        BackendService, new TBinaryProtocol.Factory)
      val server = ServerBuilder()
        .codec(ThriftServerFramedCodec())
        .bindTo(new InetSocketAddress(basePort()))
        .name("thrift")
        .build(service)
      while (true) server.synchronized { server.wait() }
    }
  }
}
