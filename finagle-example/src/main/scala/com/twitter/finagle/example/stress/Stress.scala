package com.twitter.finagle.example.stress

import com.twitter.finagle.Service
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.{Method, Request, Response, Status, Version}
import com.twitter.finagle.stats.SummarizingStatsReceiver
import com.twitter.util.{Future, Stopwatch}
import java.net.{InetSocketAddress, URI}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.collection.JavaConverters._

/**
 * A program to stress an HTTP server. The code below throttles request using an
 * asynchronous semaphore. Specify the uri, concurrency level, and the total number
 * of requests at the command line.
 */
object Stress {
  def main(args: Array[String]): Unit = {
    val uri = new URI(args(0))
    val concurrency = args(1).toInt
    val totalRequests = args(2).toInt

    val errors = new AtomicInteger(0)
    val responses = new ConcurrentHashMap[Status, AtomicLong]()

    val request = Request(Version.Http11, Method.Get, uri.getPath)
    request.headerMap.set("Host", uri.getHost)

    val statsReceiver = new SummarizingStatsReceiver

    val client: Service[Request, Response] = ClientBuilder()
      .stack(com.twitter.finagle.Http.client)
      .hosts(new InetSocketAddress(uri.getHost, uri.getPort))
      .hostConnectionCoresize(concurrency)
      .reportTo(statsReceiver)
      .retries(3)
      .hostConnectionLimit(concurrency)
      .build()

    val completedRequests = new AtomicInteger(0)

    val makeAtomicLong = new java.util.function.Function[Status, AtomicLong]() {
      def apply(s: Status): AtomicLong = new AtomicLong(0)
    }

    val requests = Future.parallel(concurrency) {
      Future.times(totalRequests / concurrency) {
        client(request)
          .onSuccess { response =>
            responses.computeIfAbsent(response.status, makeAtomicLong).incrementAndGet()
          }.handle {
            case e =>
              errors.incrementAndGet()
          }.ensure {
            completedRequests.incrementAndGet()
          }
      }
    }

    val elapsed = Stopwatch.start()

    Future.join(requests) ensure {
      client.close()

      val duration = elapsed()
      println("%20s\t%s".format("Status", "Count"))
      for ((status, count) <- responses.asScala)
        println("%20s\t%d".format(status, count.get))
      println("================")
      println(
        "%d requests completed in %dms (%f requests per second)".format(
          completedRequests.get,
          duration.inMilliseconds,
          totalRequests.toFloat / duration.inMillis.toFloat * 1000
        )
      )
      println("%d errors".format(errors.get))

      println("stats")
      println("=====")

      statsReceiver.print()
    }
  }

}
