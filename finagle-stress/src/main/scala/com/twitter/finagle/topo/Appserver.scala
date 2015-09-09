package com.twitter.finagle.topo

import com.twitter.app.App
import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.httpx.{Request, Response, Status}
import com.twitter.finagle.thrift.ThriftClientFramedCodec
import com.twitter.finagle.topo.{thriftscala => thrift}
import com.twitter.finagle.{Httpx, Resolver, Service, ThriftMux}
import com.twitter.io.Buf
import com.twitter.logging.Logging
import com.twitter.ostrich.admin.{AdminHttpService, RuntimeEnvironment}
import com.twitter.util.{Await, Duration, Future, Stopwatch, StorageUnit}
import scala.util.Random

class AppService(clients: Seq[thrift.Backend.FutureIface], responseSample: Seq[(Duration, StorageUnit)])
  extends Service[Request, Response]
{
  private[this] val rng = new Random

  private[this] def nextResponse() =
    responseSample(rng.nextInt(responseSample.size))

  def apply(req: Request) = {
    val responses = for (client <- clients) yield {
      val (latency, size) = nextResponse()
      client.request(size.inBytes.toInt, latency.inMilliseconds.toInt)
    }

    val elapsed = Stopwatch.start()

    Future.collect(responses) map { bodies =>
      val response = Response(req.version, Status.Ok)
      val bytes = (bodies mkString "").getBytes("UTF-8")
      response.content = Buf.ByteArray.Owned(bytes)
      response.headerMap.set("Content-Lenth", "%d".format(bytes.size))
      response.headerMap.set("X-Finagle-Latency-Ms", "%d".format(elapsed().inMilliseconds))
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

    val server = Httpx.serve(":"+basePort(),
      new AppService(clients.toSeq, responseSample()))
    Await.ready(server)
  }
}
