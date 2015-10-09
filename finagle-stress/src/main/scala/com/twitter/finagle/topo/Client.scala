package com.twitter.finagle.topo

import com.twitter.finagle.Service
import com.twitter.finagle.Httpx
import com.twitter.finagle.httpx.{Request, Response}
import com.twitter.logging.{Level, LoggerFactory, ConsoleHandler}
import com.twitter.ostrich.admin.{RuntimeEnvironment, AdminHttpService}

object Client {
  private[this] def go(svc: Service[Request, Response]) {
    val req = Request("/")
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

    val service = Httpx.newService(hostport)

    for (which <- 0 until n)
      go(service)
  }
}
