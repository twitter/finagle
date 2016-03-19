package com.twitter.finagle.redis.util

import java.lang.ProcessBuilder
import java.net.InetSocketAddress
import java.io.{BufferedWriter, FileWriter, PrintWriter, File}
import com.twitter.finagle.Redis
import com.twitter.finagle.redis.Client
import com.twitter.util.RandomSocket
import scala.collection.JavaConverters._
import scala.util.Random

// Helper classes for spinning up a little redis cluster
object RedisCluster { self =>
  import collection.mutable.{Stack => MutableStack}
  val instanceStack = MutableStack[ExternalRedis]()

  def address: Option[InetSocketAddress] = instanceStack.head.address
  def address(i: Int) = instanceStack(i).address
  def addresses: Seq[Option[InetSocketAddress]] = instanceStack.map { i => i.address }

  def hostAddresses(from: Int = 0, until: Int = instanceStack.size): String = {
    require(instanceStack.length > 0)
    addresses.slice(from, until).map { address =>
      val addy = address.get
      "%s:%d".format("127.0.0.1", addy.getPort())
    }.sorted.mkString(",")
  }

  def start(count: Int = 1, mode: RedisMode = RedisMode.Standalone): Seq[ExternalRedis] = {
    (0 until count).map { i =>
      start(new ExternalRedis(mode))
    }
  }

  def start(instance: ExternalRedis): ExternalRedis = {
    instance.start()
    instanceStack.push(instance)
    instance
  }

  def stop(): ExternalRedis = {
    val instance = instanceStack.pop()
    instance.stop()
    instance
  }

  def stopAll() {
    instanceStack.foreach { i => i.stop() }
    instanceStack.clear
  }

  // Make sure the process is always killed eventually
  Runtime.getRuntime().addShutdownHook(new Thread {
    override def run() {
      self.instanceStack.foreach { instance => instance.stop() }
    }
  });
}

sealed trait RedisMode
object RedisMode {
  case object Standalone extends RedisMode
  case object Sentinel extends RedisMode
  case object Cluster extends RedisMode
}
class ExternalRedis(mode: RedisMode = RedisMode.Standalone) {
  private[this] val rand = new Random
  private[this] var process: Option[Process] = None
  private[this] val forbiddenPorts = 6300.until(7300)
  var address: Option[InetSocketAddress] = None

  private[this] def assertRedisBinaryPresent() {
    val p = new ProcessBuilder("redis-server", "--help").start()
    p.waitFor()
    val exitValue = p.exitValue()
    require(exitValue == 0 || exitValue == 1, "redis-server binary must be present.")
  }

  private[this] def findAddress() {
    var tries = 100
    while (address == None && tries >= 0) {
      address = Some(RandomSocket.nextAddress())
      if (forbiddenPorts.contains(address.get.getPort)) {
        address = None
        tries -= 1
        Thread.sleep(5)
      }
    }
    address.getOrElse { sys.error("Couldn't get an address for the external redis instance") }
  }

  protected def createConfigFile(port: Int): File = {
    val f = File.createTempFile("redis-"+rand.nextInt(1000), ".tmp")
    f.deleteOnExit()
    val out = new PrintWriter(new BufferedWriter(new FileWriter(f)))
    val conf = "port %s".format(port)
    out.write(conf)
    out.println()
    out.close()
    f
  }

  def start() {
    val port = address.get.getPort()
    val conf = createConfigFile(port).getAbsolutePath
    val cmd: Seq[String] = if (mode == RedisMode.Sentinel) {
      Seq("redis-server", conf, "--sentinel")
    } else {
      Seq("redis-server", conf)
    }
    val builder = new ProcessBuilder(cmd.asJava)
    process = Some(builder.start())
    Thread.sleep(200)
  }

  def stop() {
    process.foreach { p =>
      p.destroy()
      p.waitFor()
    }
  }

  def restart() {
    stop()
    start()
  }

  def newClient(): Client = Redis.newRichClient(s"127.0.0.1:${address.get.getPort}")

  def withClient[T](f: Client => T): T = {
    val client = newClient
    try f(client) finally client.close()
  }

  assertRedisBinaryPresent()
  findAddress()
}
