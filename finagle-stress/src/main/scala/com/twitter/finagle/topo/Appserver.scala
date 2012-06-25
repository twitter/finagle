package com.twitter.finagle.topo

import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.builder.{
  ClientBuilder, Cluster, ServerBuilder, StaticCluster}
import com.twitter.finagle.http.Http
import com.twitter.finagle.stats.OstrichStatsReceiver
import com.twitter.finagle.thrift.ThriftClientFramedCodec
import com.twitter.logging.{Level, Logger, LoggerFactory, ConsoleHandler}
import com.twitter.ostrich.admin.{RuntimeEnvironment, AdminHttpService}
import com.twitter.util.{Future, Duration, Time, StorageUnit}
import java.net.{SocketAddress, InetSocketAddress}
import org.apache.thrift.protocol.TBinaryProtocol
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._
import scala.util.Random
import com.twitter.finagle.tracing.ConsoleTracer

class AppService(clients: Seq[thrift.Backend.ServiceIface], responseSample: Seq[(Duration, StorageUnit)])
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

    val begin = Time.now

    Future.collect(responses) map { bodies =>
      val response = new DefaultHttpResponse(req.getProtocolVersion, HttpResponseStatus.OK)
      val bytes = (bodies mkString "").getBytes
      response.setContent(ChannelBuffers.wrappedBuffer(bytes))
      response.setHeader("Content-Lenth", "%d".format(bytes.size))
      response.setHeader("X-Finagle-Latency-Ms", "%d".format(begin.untilNow.inMilliseconds))
      response
    }
  }
}

object Appserver {
  private[this] lazy val log = Logger(getClass)

  private[this] def mkClient(name: String, cluster: Cluster[SocketAddress]) = {
    val transport = ClientBuilder()
      .name(name)
      .cluster(cluster)
      .codec(ThriftClientFramedCodec())
      .reportTo(new OstrichStatsReceiver)
      .hostConnectionLimit(1)
      .build()

    new thrift.Backend.ServiceToClient(
      transport, new TBinaryProtocol.Factory())
  }

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
    } yield mkClient("client%d".format(i), new StaticCluster[SocketAddress]((0 until n.toInt) map { _ => addr }))

    val runtime = RuntimeEnvironment(this, Array()/*no args for you*/)
    val adminService = new AdminHttpService(basePort+1, 100/*backlog*/, runtime)
    adminService.start()

    val service = new AppService(clients.toSeq, responseSample)

    ServerBuilder()
      .name("appserver")
      .codec(Http())
      .reportTo(new OstrichStatsReceiver)
      .bindTo(new InetSocketAddress(basePort))
      .tracerFactory(ConsoleTracer.factory)
      .build(service)
  }
}
