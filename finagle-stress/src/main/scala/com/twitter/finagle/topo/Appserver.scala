package com.twitter.finagle.topo

import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.builder.{  ClientBuilder, Cluster, ServerBuilder, StaticCluster}
import com.twitter.finagle.stats.OstrichStatsReceiver
import com.twitter.finagle.thrift.ThriftClientFramedCodec
import com.twitter.finagle.tracing.ConsoleTracer
import com.twitter.finagle.{Group, ThriftMux, Http}
import com.twitter.logging.{Level, Logger, LoggerFactory, ConsoleHandler}
import com.twitter.ostrich.admin.{RuntimeEnvironment, AdminHttpService}
import com.twitter.util.{Await, Future, Duration, Stopwatch, StorageUnit}
import java.net.{SocketAddress, InetSocketAddress}
import org.apache.thrift.protocol.TBinaryProtocol
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._
import scala.util.Random

class AppService(clients: Seq[thrift.Backend.FutureIface], responseSample: Seq[(Duration, StorageUnit)])
  extends Service[HttpRequest, HttpResponse]
{
  private[this] val rng = new Random

  private[this] def nextResponse() =
    responseSample(rng.nextInt(responseSample.size))

  def apply(req: HttpRequest) = {
    val responses = for (client <- clients) yield {
      val (latency, size) = nextResponse()
      client.request(size.inBytes.toInt, latency.inMilliseconds.toInt)
    }

    val elapsed = Stopwatch.start()

    Future.collect(responses) map { bodies =>
      val response = new DefaultHttpResponse(req.getProtocolVersion, HttpResponseStatus.OK)
      val bytes = (bodies mkString "").getBytes
      response.setContent(ChannelBuffers.wrappedBuffer(bytes))
      response.setHeader("Content-Lenth", "%d".format(bytes.size))
      response.setHeader("X-Finagle-Latency-Ms", "%d".format(elapsed().inMilliseconds))
      response
    }
  }
}

object Appserver {
  private[this] lazy val log = Logger(getClass)

  private[this] def usage() {
    System.err.println("Server basePort responseSample n*hostport [k*hostport..]")
    System.exit(1)
  }

  def main(args: Array[String]) = {
    LoggerFactory(
      node = "",
      level = Some(Level.INFO),
      handlers = ConsoleHandler() :: Nil
    ).apply()

    if (args.size < 3)
      usage()

    val basePort = args(0).toInt
    // eg. generate with jot -r 160 1000 10000 | rs  0 2 | awk '{print 100*$1 ":" int($2/100)}' | tr '\n' ','

    val responseSample = for {
      sample <- args(1) split ","
      Array(size, latency) = sample split ":"
    } yield (latency.toInt.milliseconds, size.toInt.bytes)

    val clients = for {
      (spec, i) <- (args drop 2).zipWithIndex
      Array(n, hostport) = spec split "\\*"
      Array(host, port) = hostport split ":"
      addr = new InetSocketAddress(host, port.toInt)
    } yield ThriftMux.newIface[thrift.Backend.FutureIface](
      Group[SocketAddress](Seq.fill(n.toInt)(addr):_*).named("mux%d".format(i)))

    val runtime = RuntimeEnvironment(this, Array()/*no args for you*/)
    val adminService = new AdminHttpService(basePort+1, 100/*backlog*/, runtime)
    adminService.start()

    val server = Http.serve(":"+args(0), new AppService(clients.toSeq, responseSample))
    Await.ready(server)
  }
}
