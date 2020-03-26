package com.twitter.finagle.redis.util

import java.net.{InetSocketAddress, Socket}
import java.io.{BufferedWriter, File, FileWriter, PrintWriter}
import com.twitter.finagle.Redis
import com.twitter.finagle.redis.Client
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.util.Random

object RedisTestHelper {
  private val SemVerRegex = "(\\d+)\\.(\\d+)\\.(\\d+)".r

  lazy val redisServerExists: Boolean = redisServerVersion.isDefined

  lazy val redisServerVersion: Option[(Int, Int, Int)] = {
    try {
      val p = new ProcessBuilder("redis-server", "--version").start()
      p.waitFor(10, TimeUnit.SECONDS)
      val exitValue = p.exitValue()
      if (exitValue != 0 && exitValue != 1) None
      else {
        // An example output from redis 5.0.1:
        // `Redis server v=5.0.1 sha=00000000:0 malloc=libc bits=64 build=c6d614f40d59bf0a`
        val stdout = scala.io.Source.fromInputStream(p.getInputStream()).mkString
        val version = stdout.split(" ")(2).substring(2)

        version match {
          case SemVerRegex(a, b, c) => Some((a.toInt, b.toInt, c.toInt))
          case _ => None
        }
      }
    } catch {
      case _: Exception => None
    }
  }
}

// Helper classes for spinning up a little redis cluster
object RedisCluster { self =>
  import collection.mutable.{Stack => MutableStack}
  val instanceStack = MutableStack[ExternalRedis]()

  def address: Option[InetSocketAddress] = instanceStack.head.address
  def address(i: Int) = instanceStack(i).address
  def addresses: Seq[Option[InetSocketAddress]] =
    instanceStack.map { i => i.address }.toList

  def hostAddresses(from: Int = 0, until: Int = instanceStack.size): String = {
    require(instanceStack.nonEmpty)
    addresses
      .slice(from, until)
      .map { address =>
        val addy = address.get
        "%s:%d".format("127.0.0.1", addy.getPort())
      }
      .sorted
      .mkString(",")
  }

  def start(count: Int = 1, mode: RedisMode = RedisMode.Standalone): Seq[ExternalRedis] = {
    (0 until count).map { i => start(new ExternalRedis(mode)) }
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

  def stopAll(): Unit = {
    instanceStack.foreach { i => i.stop() }
    instanceStack.clear
  }

  // Make sure the process is always killed eventually
  Runtime
    .getRuntime()
    .addShutdownHook(new Thread {
      override def run(): Unit = {
        self.instanceStack.foreach { instance => instance.stop() }
      }
    })
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
  private[this] val possiblePorts = 49152.until(55535)
  var address: Option[InetSocketAddress] = None

  private[this] def assertRedisBinaryPresent(): Unit = {
    require(RedisTestHelper.redisServerExists, "redis-server binary must be present.")
  }

  private[this] def findAddress(): Unit = {
    var tries = possiblePorts.size - 1
    while (address.isEmpty && tries >= 0) {
      val addr = new InetSocketAddress(possiblePorts(tries))
      val socket = new Socket

      try {
        socket.setReuseAddress(true)
        socket.bind(addr)
        address = Some(addr)
      } catch {
        case exc: Exception =>
          address = None
          tries -= 1
          Thread.sleep(5)
      } finally {
        socket.close()
      }
    }
    address.getOrElse { sys.error("Couldn't get an address for the external redis instance") }
  }

  protected def createConfigFile(port: Int): File = {
    val confFile = File.createTempFile("redis-" + rand.nextInt(1000), ".tmp")
    val nodesFile = File.createTempFile("redis-nodes-" + rand.nextInt(1000), ".tmp")
    val appendFile = File.createTempFile("redis-append-" + rand.nextInt(1000), ".aof")
    val dbFile = File.createTempFile("redis-db-" + rand.nextInt(1000), ".db")

    confFile.deleteOnExit()
    nodesFile.deleteOnExit()
    appendFile.deleteOnExit()
    dbFile.deleteOnExit()

    val out = new PrintWriter(new BufferedWriter(new FileWriter(confFile)))
    var conf = "port %s".format(port)

    if (mode == RedisMode.Cluster) {
      conf += s"""
cluster-enabled yes
cluster-config-file ${nodesFile.getAbsolutePath}
cluster-node-timeout 5000
appendonly yes
dir ${appendFile.getParent}
appendfilename ${appendFile.getName}
dbfilename ${dbFile.getName}
"""
    }

    out.write(conf)
    out.println()
    out.close()
    confFile
  }

  def start(): Unit = {
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

  def stop(): Unit = {
    process.foreach { p =>
      p.destroy()
      p.waitFor()
    }
  }

  def restart(): Unit = {
    stop()
    start()
  }

  def newClient(): Client = Redis.newRichClient(s"127.0.0.1:${address.get.getPort}")

  def withClient[T](f: Client => T): T = {
    val client = newClient
    try f(client)
    finally client.close()
  }

  assertRedisBinaryPresent()
  findAddress()
}
