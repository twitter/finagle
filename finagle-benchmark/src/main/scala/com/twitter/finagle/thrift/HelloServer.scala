package com.twitter.finagle.thrift

import com.twitter.finagle.ThriftMux
import com.twitter.finagle.benchmark.thriftscala._

import com.twitter.util.{Await, Future}

/**
 * Thrift server for [[HelloClient]] (thrift allocations benchmark).
 */
object HelloServer {
  def main(args: Array[String]): Unit = {
    val server = ThriftMux.server.serveIface(
      "localhost:1234",
      new Hello.MethodPerEndpoint {
        def echo(m: String) = {
          Future.value(m)
        }
      })

    Await.ready(server)
  }
}
