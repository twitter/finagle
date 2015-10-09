package com.twitter.finagle.example.memcache

import com.twitter.app.Flag
import com.twitter.app.App
import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.memcached
import com.twitter.finagle.memcached.protocol.text.Memcached
import com.twitter.finagle.stats.OstrichStatsReceiver
import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.io.Buf
import com.twitter.ostrich.admin.{RuntimeEnvironment, AdminHttpService}
import com.twitter.util.{Future, Stopwatch}
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory

class PersistentService[Req, Rep](factory: ServiceFactory[Req, Rep]) extends Service[Req, Rep] {
  @volatile private[this] var currentService: Future[Service[Req, Rep]] = factory()

  def apply(req: Req) =
    currentService flatMap { service =>
      service(req) onFailure { _ =>
        currentService = factory()
      }
    }
}

object MemcacheStress extends App {
  private[this] val config = new {
    val concurrency: Flag[Int] = flag("concurrency", 400, "concurrency")
    val hosts: Flag[String] = flag("hosts", "localhost:11211", "hosts")
    val keysize: Flag[Int] = flag("keysize", 55, "keysize")
    val valuesize: Flag[Int] = flag("valuesize", 1, "valuesize")
    val nworkers: Flag[Int] = flag("nworkers", -1, "nworkers")
    val stats: Flag[Boolean] = flag("stats", true, "stats")
    val tracing: Flag[Boolean] = flag("tracing", true, "tracing")
  }
  val count = new AtomicLong

  def proc(client: memcached.Client, key: String, value: Buf) {
    client.set(key, value) ensure {
      count.incrementAndGet()
      proc(client, key, value)
    }
  }

  def main() {
    var builder = ClientBuilder()
      .name("mc")
      .codec(Memcached())
      .hostConnectionLimit(config.concurrency())
      .hosts(config.hosts())

    if (config.nworkers() > 0)
      builder = builder.channelFactory(
          new NioClientSocketChannelFactory(
            Executors.newCachedThreadPool(new NamedPoolThreadFactory("memcacheboss")),
            Executors.newCachedThreadPool(new NamedPoolThreadFactory("memcacheIO")),
            config.nworkers()
          )
        )

    if (config.stats())    builder = builder.reportTo(new OstrichStatsReceiver)
    if (config.tracing())  com.twitter.finagle.tracing.Trace.enable()
    else                 com.twitter.finagle.tracing.Trace.disable()

    val key = "x" * config.keysize()
    val value = Buf.Utf8("y" * config.valuesize())

    val runtime = RuntimeEnvironment(this, Array()/*no args for you*/)
    val adminService = new AdminHttpService(2000, 100/*backlog*/, runtime)
    adminService.start()

    println(builder)
    val factory = builder.buildFactory()
    val elapsed = Stopwatch.start()

    for (_ <- 0 until config.concurrency()) {
      val svc = new PersistentService(factory)
      val client = memcached.Client(svc)
      proc(client, key, value)
    }

    while (true) {
      Thread.sleep(5000)

      val howlong = elapsed()
      val howmuch = count.get()
      assert(howmuch > 0)

      printf("%d QPS\n", howmuch / howlong.inSeconds)
    }
  }
}
