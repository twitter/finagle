package com.twitter.finagle.integration

import java.net.SocketAddress
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel._
import org.jboss.netty.channel.group.DefaultChannelGroup
import org.jboss.netty.handler.codec.http._

import org.specs.Specification

import com.twitter.conversions.time._
import com.twitter.util.{Return, Throw}
import com.twitter.killdeer.{Cdf, DotsHandler}

import com.twitter.ostrich.{StatsCollection, StatsProvider}

import com.twitter.finagle.service.Service
import com.twitter.finagle.builder.{ClientBuilder, Http}

object LoadBalancerIntegrationSpec extends Specification {
  def prettyPrintStats(stats: StatsProvider) {
    stats.getCounterStats foreach { case (name, count) =>
      println("# %-15s %d".format(name, count))
    }
  }

  "Load Balancer" should {
    val servers = (0 until 3).toArray map(_ => EmbeddedServer())
    val stats = new StatsCollection

    // TODO: parallelize these; measure throughput.

    doAfter {
      println("> stats")
      prettyPrintStats(stats)

      servers.zipWithIndex foreach { case (server, which) =>
        server.stop()
        println("> SERVER[%d]".format(which))
        prettyPrintStats(server.stats)
      }
    }

    def runTest[A](client: Service[HttpRequest, HttpResponse])(f: PartialFunction[Int, Unit]) {
      0 until 1000 foreach { i =>
        if (f.isDefinedAt(i))
          f(i)

        val future = client(
          new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/"))

        future.within(10.seconds) match {
          case Return(_) =>
            stats.incr("success")
          case Throw(_) =>
            stats.incr("fail")
        }
      }
    }

    "balance[1]" in {
      val client = ClientBuilder()
        .codec(Http)
        .hosts(servers map(_.addr))
        .buildService[HttpRequest, HttpResponse]

      runTest(client) {
        case 100 =>
          servers(1).stop()
      }

      true must beTrue
    }

    "balance[2]" in {
      val client = ClientBuilder()
        .codec(Http)
        .hosts(servers map(_.addr))
        .requestTimeout(10.milliseconds)
        .buildService[HttpRequest, HttpResponse]

      runTest(client) {
        case 100 =>
          servers(1).becomeApplicationNonresponsive()
      }

      true must beTrue
    }

    "balance[3]" in {
      val client = ClientBuilder()
        .codec(Http)
        .hosts(servers map(_.addr))
        .buildService[HttpRequest, HttpResponse]

      runTest(client) {
        case 100 =>
          servers(1).becomeConnectionNonresponsive()
      }

      true must beTrue
    }
  }
}
