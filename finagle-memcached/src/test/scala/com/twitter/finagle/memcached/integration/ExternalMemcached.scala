package com.twitter.finagle.memcached.integration

import java.lang.ProcessBuilder
import java.net.{BindException, ServerSocket, InetSocketAddress}
import com.twitter.util.{Try, Stopwatch, Duration, RandomSocket}
import com.twitter.conversions.time._
import collection.JavaConversions._
import scala.collection._

object ExternalMemcached { self =>
  class MemcachedBinaryNotFound extends Exception
  private[this] var processes: Map[InetSocketAddress, Process] = mutable.Map()
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
    if (address==None) sys.error("Couldn't get an address for the external memcached")

    takenPorts += (address.getOrElse(new InetSocketAddress(0))).getPort
    address
  }

  def start(address: Option[InetSocketAddress] = None): Option[InetSocketAddress] = {
    def exec(address: InetSocketAddress) {
      val cmd = Seq("memcached", "-l", address.getHostName,
        "-p", address.getPort.toString)
      val builder = new ProcessBuilder(cmd.toList)
      processes += (address -> builder.start())
    }

    (address orElse findAddress()) flatMap { addr =>
      try {
        exec(addr)
        if (waitForPort(addr.getPort))
          Some(addr)
        else
          None
      } catch {
        case _: Throwable => None
      }
    }
  }

  def waitForPort(port: Int, timeout: Duration = 5.seconds): Boolean = {
    val elapsed = Stopwatch.start()
    def loop(): Boolean = {
      if (! isPortAvailable(port))
        true
      else if (timeout < elapsed())
        false
      else {
        Thread.sleep(100)
        loop()
      }
    }
    loop()
  }

  def isPortAvailable(port: Int): Boolean = {
    var ss: ServerSocket = null
    var result = false
    try {
      ss = new ServerSocket(port)
      ss.setReuseAddress(true)
      result = true
    } catch { case ex: BindException =>
      result = (ex.getMessage != "Address already in use")
    } finally {
      if (ss != null)
        ss.close()
    }

    result
  }

  def stop(addr: Option[InetSocketAddress] = None) {
    if (!addr.isDefined) {
      processes.values foreach {
        p =>
          p.destroy()
          p.waitFor()
      }
    } else {
      processes(addr.get).destroy()
      processes(addr.get).waitFor()
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

  //assertMemcachedBinaryPresent()
}
