package com.twitter.finagle.topo

import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.builder.{  ClientBuilder, Cluster, ServerBuilder, StaticCluster}
import com.twitter.finagle.stats.OstrichStatsReceiver
import com.twitter.finagle.thrift.ThriftClientFramedCodec
import com.twitter.finagle.tracing.ConsoleTracer
import com.twitter.finagle.{Resolver, ThriftMux, Http}
import com.twitter.logging.{Level, Logger, LoggerFactory, ConsoleHandler}
import com.twitter.ostrich.admin.{RuntimeEnvironment, AdminHttpService}
import com.twitter.util.{Await, Future, Duration, Stopwatch, StorageUnit}
import java.net.{SocketAddress, InetSocketAddress}
import org.apache.thrift.protocol.TBinaryProtocol
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http.{HttpRequest=>HttpAsk, _}
import scala.util.Random
import com.twitter.app.App
import com.twitter.logging.Logging
import com.twitter.finagle.topo.{thriftscala => thrift}

class AppService(clients: Seq[thrift.Backend.FutureIface], responseSample: Seq[(Duration, StorageUnit)])
  extends Service[HttpAsk, HttpResponse]
{
  private[this] val rng = new Random

  private[this] def nextResponse() =
    responseSample(rng.nextInt(responseSample.size))

  def apply(req: HttpAsk) = {
    val responses = for (client <- clients) yield {
      val (latency, size) = nextResponse()
      client.request(size.inBytes.toInt, latency.inMilliseconds.toInt)
    }

    val elapsed = Stopwatch.start()

    Future.collect(responses) map { bodies =>
      val response = new DefaultHttpResponse(req.getProtocolVersion, HttpResponseStatus.OK)
      val bytes = (bodies mkString "").getBytes
      response.setContent(ChannelBuffers.wrappedBuffer(bytes))
      response.headers.set("Content-Lenth", "%d".format(bytes.size))
      response.headers.set("X-Finagle-Latency-Ms", "%d".format(elapsed().inMilliseconds))
      response
    }
  }
}

object Appserver extends App with Logging {
  val useThriftmux = flag("thriftmux", true, "Use thriftmux")
  val basePort = flag("baseport", 3000, "Base port")
  val responseSample = flag("responsesample",
    Seq((0.millisecond, 0.bytes)), "Response sample")
  val clientAddrs = flag("clients", Seq(":2000", ":2020"), "clients")

  def main() {
    val clients = for {
      (d, i) <- clientAddrs().zipWithIndex
      dest = Resolver.eval(d)
    } yield {
      if (useThriftmux())
        ThriftMux.newIface[thrift.Backend.FutureIface](dest, "mux%d".format(i))
      else {
        val transport = ClientBuilder()
          .dest(dest)
          .name("mux%d".format(i))
          .codec(ThriftClientFramedCodec())
          .hostConnectionLimit(1000)
          .build()
        new thrift.Backend.FinagledClient(transport)
      }
    }

    val runtime = RuntimeEnvironment(this, Array()/*no args for you*/)
    val adminService = new AdminHttpService(basePort()+1, 100/*backlog*/, runtime)
    adminService.start()

    val server = Http.serve(":"+basePort(),
      new AppService(clients.toSeq, responseSample()))
    Await.ready(server)
  }
}
