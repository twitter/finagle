package com.twitter.finagle.memcached.integration

import java.lang.ProcessBuilder
import java.net.InetSocketAddress
import com.twitter.util.RandomSocket
import collection.JavaConversions._

object ExternalMemcached { self =>
  class MemcachedBinaryNotFound extends Exception
  private[this] var process: Process = null
  private[this] val forbiddenPorts = 11000.until(11900)
  var address: Option[InetSocketAddress] = None
  // prevent us from taking a port that is anything close to a real memcached port.

  private[this] def assertMemcachedBinaryPresent() {
    val p = new ProcessBuilder("memcached", "-help").start()
    p.waitFor()
    require(p.exitValue() == 0, "memcached binary must be present.")
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
    address.getOrElse { error("Couldn't get an address for the external memcached") }
  }

  def start() {
    val cmd: Seq[String] = Seq("memcached",
                    "-l", address.get.getHostName(),
                    "-p", address.get.getPort().toString)

    val builder = new ProcessBuilder(cmd.toList)
    process = builder.start()
    Thread.sleep(100)
  }

  def stop() {
    if (process != null) {
      process.destroy()
      process.waitFor()
    }
  }

  def restart() {
    stop()
    start()
  }

  // Make sure the process is always killed eventually
  Runtime.getRuntime().addShutdownHook(new Thread {
    override def run() {
      self.stop()
    }
  });

  assertMemcachedBinaryPresent()
  findAddress()
}
