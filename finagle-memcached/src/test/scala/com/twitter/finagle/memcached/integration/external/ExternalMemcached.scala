package com.twitter.finagle.memcached.integration.external

import com.twitter.conversions.DurationOps._
import com.twitter.util.Duration
import com.twitter.util.RandomSocket
import com.twitter.util.Stopwatch
import java.net.BindException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import scala.jdk.CollectionConverters
import scala.collection._
import scala.collection.immutable.Stream
import scala.util.control.NonFatal

object TestMemcachedServer {
  def start(): Option[TestMemcachedServer] = start(None)

  def start(address: Option[InetSocketAddress]): Option[TestMemcachedServer] = {
    Option(System.getProperty("EXTERNAL_MEMCACHED_PATH")) match {
      case Some(externalMemcachedPath) =>
        ExternalMemcached.start(address, externalMemcachedPath)
      case None =>
        InternalMemcached.start(address)
    }
  }
}

trait TestMemcachedServer {
  val address: InetSocketAddress
  def stop(): Unit
}

private[memcached] object InternalMemcached {
  def start(address: Option[InetSocketAddress]): Option[TestMemcachedServer] = {
    try {
      val server = new InProcessMemcached(
        address.getOrElse(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
      )
      Some(new TestMemcachedServer {
        val address = server.start().boundAddress.asInstanceOf[InetSocketAddress]
        def stop(): Unit = { server.stop(true) }
      })
    } catch {
      case NonFatal(_) => None
    }
  }
}

private[memcached] object ExternalMemcached { self =>
  class MemcachedBinaryNotFound extends Exception
  private[this] var processes: List[Process] = List()
  private[this] val forbiddenPorts = 11000.until(11900)
  private[this] var takenPorts: Set[Int] = Set[Int]()
  // prevent us from taking a port that is anything close to a real memcached port.

  private[this] def findAddress() = {
    var address: Option[InetSocketAddress] = None
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
    if (address == None) sys.error("Couldn't get an address for the external memcached")

    takenPorts += address
      .getOrElse(
        new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
      )
      .getPort
    address
  }

  def start(
    address: Option[InetSocketAddress],
    externalMemcachedPath: String
  ): Option[TestMemcachedServer] = {
    def exec(address: InetSocketAddress): Process = {
      val cmd =
        List(externalMemcachedPath, "-l", address.getHostName, "-p", address.getPort.toString)
      val builder = new ProcessBuilder(cmd: _*)
      builder.start()
    }

    (address orElse findAddress()) flatMap { addr =>
      try {
        val proc = exec(addr)
        processes :+= proc

        if (waitForPort(addr.getPort))
          Some(new TestMemcachedServer {
            val address = addr
            def stop(): Unit = {
              proc.destroy()
              proc.waitFor()
            }
          })
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
      if (!isPortAvailable(port))
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
    } catch {
      case ex: BindException =>
        result = !ex.getMessage.contains("Address already in use")
    } finally {
      if (ss != null)
        ss.close()
    }

    result
  }

  // Make sure the process is always killed eventually
  Runtime
    .getRuntime()
    .addShutdownHook(new Thread {
      override def run(): Unit = {
        processes foreach { p =>
          p.destroy()
          p.waitFor()
        }
      }
    })
}
