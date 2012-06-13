package com.twitter.finagle.memcached.integration

import java.lang.ProcessBuilder
import java.net.InetSocketAddress
import com.twitter.util.RandomSocket
import collection.JavaConversions._

object ExternalMemcached { self =>
  class MemcachedBinaryNotFound extends Exception
  private[this] var processes: List[Process] = List()
  private[this] val forbiddenPorts = 11000.until(11900)
  private[this] var takenPorts: Set[Int] = Set[Int]()
  // prevent us from taking a port that is anything close to a real memcached port.

  private[this] def assertMemcachedBinaryPresent() {
    val p = new ProcessBuilder("memcached", "-help").start()
    p.waitFor()
    require(p.exitValue() == 0, "memcached binary must be present.")
  }

  private[this] def findAddress() = {
    var address : Option[InetSocketAddress] = None
    var tries = 100
    while (address == None && tries >= 0) {
      address = Some(RandomSocket.nextAddress())
      if (forbiddenPorts.contains(address.get.getPort) ||
            takenPorts.contains(address.get.getPort)) {
        address = None
        tries -= 1
        Thread.sleep(5)
      }
    }
    if (address==None) error("Couldn't get an address for the external memcached")

    takenPorts += (address.getOrElse(new InetSocketAddress(0))).getPort
    address
  }

  def start(addr:Option[InetSocketAddress] = None): Option[InetSocketAddress] = {
    val address:InetSocketAddress = addr getOrElse { findAddress().get }

    val cmd: Seq[String] = Seq("memcached",
                    "-l", address.getHostName(),
                    "-p", address.getPort().toString)

    val builder = new ProcessBuilder(cmd.toList)
    processes ::= builder.start()
    Thread.sleep(100)

    Some(address)
  }

  def stop() {
    processes foreach { p =>
      p.destroy()
      p.waitFor()
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
}
