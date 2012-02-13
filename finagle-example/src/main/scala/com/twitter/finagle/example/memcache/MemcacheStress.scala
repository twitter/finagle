package com.twitter.finagle.example.memcache

import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.builder.{
  ReferenceCountedChannelFactory, LazyRevivableChannelFactory}
import com.twitter.finagle.memcached
import com.twitter.finagle.memcached.protocol.text.Memcached
import com.twitter.finagle.stats.OstrichStatsReceiver
import com.twitter.ostrich.admin.{RuntimeEnvironment, AdminHttpService}
import com.twitter.util.Time
import java.net.InetSocketAddress
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory

object MemcacheStress {
  val count = new AtomicLong
  
  def proc(client: memcached.Client, key: String, value: ChannelBuffer) {
    client.set(key, value) ensure {
      count.incrementAndGet()
      proc(client, key, value)
    }
  }

  def main(args: Array[String]) {
    var concurrency = 400
    var hosts = "localhost:11211"
    var keysize = 55
    var valuesize = 1

    val kvs = for (a <- args; Array(k, v) = a.split("=")) yield (k, v)
    kvs foreach {
      case ("concurrency", n) =>
        concurrency = n.toInt
      case ("hosts", hs) =>
        hosts = hs
      case _ => ()
    }

    var builder = ClientBuilder()
      .name("mc")
      .codec(Memcached())
      .hostConnectionLimit(concurrency)
      .hosts(hosts)

    kvs foreach {
      case ("nworkers", n) =>
        builder = builder.channelFactory(
          new ReferenceCountedChannelFactory(
            new LazyRevivableChannelFactory(() =>
              new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(new NamedPoolThreadFactory("memcacheboss")),
                Executors.newCachedThreadPool(new NamedPoolThreadFactory("memcacheIO")),
                n.toInt
              )
            )
          )
        )

      case ("stats", x) if x != "no" =>
        builder = builder.reportTo(new OstrichStatsReceiver)

      case ("tracing", "yes") =>
        com.twitter.finagle.tracing.Trace.enable()

      case ("tracing", "no") =>
        com.twitter.finagle.tracing.Trace.disable()

      case _ => ()
    }

    val key = "x"*keysize
    val value = ChannelBuffers.wrappedBuffer(("y"*valuesize).getBytes)

    val runtime = RuntimeEnvironment(this, Array()/*no args for you*/)
    val adminService = new AdminHttpService(2000, 100/*backlog*/, runtime)
    adminService.start()

    println(builder)
    val svc = builder.build()

    val client = memcached.Client(svc)
    val begin = Time.now

    for (_ <- 0 until concurrency)
      proc(client, key, value)

    while (true) {
      Thread.sleep(5000)

      val howlong = Time.now - begin
      val howmuch = count.get()
      assert(howmuch > 0)

      printf("%d QPS\n", howmuch / howlong.inSeconds)
    }
  }
}
