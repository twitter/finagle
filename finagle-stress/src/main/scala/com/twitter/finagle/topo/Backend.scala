package com.twitter.finagle.topo

import com.twitter.conversions.time._
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.stats.OstrichStatsReceiver
import com.twitter.finagle.thrift.ThriftServerFramedCodec
import com.twitter.logging.{Level, Logger, LoggerFactory, ConsoleHandler}
import com.twitter.ostrich.admin.{RuntimeEnvironment, AdminHttpService}
import com.twitter.util.{Future, JavaTimer}
import java.net.InetSocketAddress
import org.apache.thrift.protocol.TBinaryProtocol

object BackendService extends thrift.Backend.FutureIface {
  private[this] val timer = new JavaTimer

  private[this] def makeResponse(size: Int) = "."*size

  def request(size: Int, latencyMs: Int) =
    if (latencyMs <= 0)
      Future.value(makeResponse(size))
    else
      timer.doLater(latencyMs.milliseconds) { makeResponse(size) }
}

object Backendserver {
  private[this] val log = Logger(getClass)

  def main(args: Array[String]) = {
    LoggerFactory(
      node = "",
      level = Some(Level.INFO),
      handlers = ConsoleHandler() :: Nil
    ).apply()

    if (args.size != 1) {
      log.fatal("Server port")
      System.exit(1)
    }

    val basePort = args(0).toInt

    val runtime = RuntimeEnvironment(this, Array()/*no args for you*/)
    val adminService = new AdminHttpService(basePort+1, 100/*backlog*/, runtime)
    adminService.start()

    ServerBuilder()
      .name("backend")
      .codec(ThriftServerFramedCodec())
      .reportTo(new OstrichStatsReceiver)
      .bindTo(new InetSocketAddress(basePort))
      .build(new thrift.Backend.FinagledService(BackendService, new TBinaryProtocol.Factory()))
  }
}
