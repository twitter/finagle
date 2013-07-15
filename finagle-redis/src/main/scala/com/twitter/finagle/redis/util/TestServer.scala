package com.twitter.finagle.redis
package util

import java.lang.ProcessBuilder
import java.net.InetSocketAddress
import java.io.{BufferedWriter, FileWriter, PrintWriter, File}
import com.twitter.util.RandomSocket
import collection.JavaConversions._
import scala.util.Random

// Helper classes for spinning up a little redis cluster
object RedisCluster { self =>
  import collection.mutable.{Stack => MutableStack}
  val instanceStack = MutableStack[ExternalRedis]()

  def address: Option[InetSocketAddress] = instanceStack.head.address
  def address(i: Int) = instanceStack(i).address
  def addresses: Seq[Option[InetSocketAddress]] = instanceStack.map { i => i.address }

  def hostAddresses(): String = {
    require(instanceStack.length > 0)
    addresses.map { address =>
      val addy = address.get
      "%s:%d".format(addy.getHostName(), addy.getPort())
    }.sorted.mkString(",")
  }

  def start(count: Int = 1) {
    0 until count foreach { i =>
      val instance = new ExternalRedis()
      instance.start()
      instanceStack.push(instance)
    }
  }
  def stop() {
    instanceStack.pop().stop()
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

class ExternalRedis() {
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
    val cmd: Seq[String] = Seq("redis-server", conf)
    val builder = new ProcessBuilder(cmd.toList)
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

  assertRedisBinaryPresent()
  findAddress()
}
