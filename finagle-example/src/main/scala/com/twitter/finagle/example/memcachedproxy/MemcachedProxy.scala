package com.twitter.finagle.example.memcachedproxy

import com.twitter.finagle.ListeningServer
import com.twitter.finagle.Memcached
import com.twitter.finagle.memcached.protocol.{Command, Response}
import com.twitter.finagle.Service
import java.net.{ConnectException, Socket}

/**
 * Run a server on port 8080 that delegates all Memcached requests to a server
 * running on port 11211. This Proxy is protocol aware (it decodes the Memcached
 * protocol from the client and from the backend server); since the Proxy knows
 * message boundaries, it can easily multiplex requests (e.g., for cache
 * replication) or load-balance across replicas.
 */
object MemcachedProxy {
  def main(args: Array[String]): Unit = {
    assertMemcachedRunning()

    val client: Service[Command, Response] = Memcached.client.newService("localhost:11211")

    val proxyService = new Service[Command, Response] {
      def apply(request: Command) = client(request)
    }

    val server: ListeningServer = Memcached.server
      .withLabel("memcachedproxy")
      .serve("localhost:8080", proxyService)
  }

  private[this] def assertMemcachedRunning(): Unit = {
    try {
      new Socket("localhost", 11211)
    } catch {
      case e: ConnectException =>
        println("Error: memcached must be running on port 11211")
        System.exit(1)
    }

  }
}
