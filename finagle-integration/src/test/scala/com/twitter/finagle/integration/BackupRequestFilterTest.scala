package com.twitter.finagle.integration

import com.twitter.conversions.DurationOps._
import com.twitter.conversions.PercentOps._
import com.twitter.finagle.Address
import com.twitter.finagle.context.BackupRequest
import com.twitter.finagle.integration.thriftscala.Echo
import com.twitter.finagle.service.RetryBudget
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.tracing.Annotation
import com.twitter.finagle.tracing.Record
import com.twitter.finagle.tracing.TraceId
import com.twitter.finagle.tracing.Tracer
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.Http
import com.twitter.finagle.ListeningServer
import com.twitter.finagle.Name
import com.twitter.finagle.Service
import com.twitter.finagle.ThriftMux
import com.twitter.finagle.addr.WeightedAddress
import com.twitter.finagle.http
import com.twitter.finagle.partitioning.zk.ZkMetadata
import com.twitter.finagle.thrift.exp.partitioning.MethodBuilderCustomStrategy
import com.twitter.finagle.thrift.exp.partitioning.PartitioningStrategy.ResponseMerger
import com.twitter.util.Await
import com.twitter.util.Future
import com.twitter.util.Return
import com.twitter.util.Throw
import java.net.InetAddress
import java.net.InetSocketAddress
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.IntegrationPatience
import org.scalatest.funsuite.AnyFunSuite

class BackupRequestFilterTest extends AnyFunSuite with Eventually with IntegrationPatience {

  private def await[T](f: Future[T]): T = Await.result(f, 15.seconds)

  private def newTracer(messages: LinkedBlockingQueue[String]): Tracer = new Tracer {
    def record(record: Record): Unit = {
      record match {
        case Record(_, _, Annotation.Message(value), _) =>
          messages.put(value)
        case Record(_, _, Annotation.BinaryAnnotation(key, _), _) =>
          messages.put(key)
        case _ =>
      }
    }
    def sampleTrace(traceId: TraceId): Option[Boolean] = Tracer.SomeTrue
    def getSampleRate: Float = 1f
  }

  private def assertServerTracerMessages(messages: LinkedBlockingQueue[String]): Unit = {
    assert(messages.contains("srv/backup_request_processing"))
  }

  private def assertClientTracerMessages(messages: LinkedBlockingQueue[String]): Unit = {
    assert(messages.contains("Client Backup Request Issued"))
    assert(
      messages.contains("Client Backup Request Won")
        || messages.contains("Client Backup Request Lost"))
    assert(messages.contains("clnt/backup_request_threshold_ms"))
    assert(messages.contains("clnt/backup_request_span_id"))
  }

  test("Http client propagates BackupRequest context") {
    val goSlow = new AtomicBoolean(false)
    val backupsSeen = new AtomicInteger(0)
    val service = Service.mk { request: http.Request =>
      val response = Future.value(http.Response())
      if (BackupRequest.wasInitiated) {
        backupsSeen.incrementAndGet()
      }
      val slow = goSlow.get
      goSlow.set(!slow)
      if (slow) {
        response.delayed(100.milliseconds)(DefaultTimer)
      } else {
        response
      }
    }

    val serverMessages = new LinkedBlockingQueue[String]()
    val server = Http.server
      .withTracer(newTracer(serverMessages))
      .serve("localhost:*", service)
    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]

    val clientMessages = new LinkedBlockingQueue[String]()
    val statsRecv = new InMemoryStatsReceiver()
    val client = Http.client
      .withTracer(newTracer(clientMessages))
      .withStatsReceiver(statsRecv)
      .withRetryBudget(RetryBudget.Infinite)
      .withLabel("backend")
      .methodBuilder(s"${addr.getHostName}:${addr.getPort}")
      .idempotent(99.percent)
      .newService

    // warm up the backup filter to have some data points.
    0.until(100).foreach { i =>
      withClue(s"warmup $i") {
        await(client(http.Request("/")))
      }
    }

    // capture state and tee it up.
    serverMessages.clear()
    clientMessages.clear()
    goSlow.set(true)
    val counter = statsRecv.counter("backend", "backups", "backups_sent")
    val backupsBefore = counter()
    val backupsSeenBefore = backupsSeen.get
    await(client(http.Request("/")))
    assert(backupsSeen.get == backupsSeenBefore + 1)
    eventually {
      assert(counter() == backupsBefore + 1)
      assertClientTracerMessages(clientMessages)
      assertServerTracerMessages(serverMessages)
    }
  }

  test("ThriftMux client propagates BackupRequest context") {
    val goSlow = new AtomicBoolean(false)
    val backupsSeen = new AtomicInteger(0)
    val service = new Echo.MethodPerEndpoint {
      def echo(msg: String): Future[String] = {
        val response = Future.value(msg)
        if (BackupRequest.wasInitiated) {
          backupsSeen.incrementAndGet()
        }
        val slow = goSlow.get
        goSlow.set(!slow)
        if (slow) {
          response.delayed(100.milliseconds)(DefaultTimer)
        } else {
          response
        }
      }
    }

    val serverMessages = new LinkedBlockingQueue[String]()
    val server = ThriftMux.server
      .withTracer(newTracer(serverMessages))
      .serveIface("localhost:*", service)
    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]

    val clientMessages = new LinkedBlockingQueue[String]()
    val statsRecv = new InMemoryStatsReceiver()
    val client = ThriftMux.client
      .withTracer(newTracer(clientMessages))
      .withStatsReceiver(statsRecv)
      .withRetryBudget(RetryBudget.Infinite)
      .withLabel("backend")
      .methodBuilder(s"${addr.getHostName}:${addr.getPort}")
      .idempotent(99.percent)
      .servicePerEndpoint[Echo.ServicePerEndpoint]

    // warm up the backup filter to have some data points.
    0.until(100).foreach { i =>
      withClue(s"warmup $i") {
        await(client.echo(Echo.Echo.Args("hi")))
      }
    }

    // capture state and tee it up.
    serverMessages.clear()
    clientMessages.clear()
    goSlow.set(true)
    val counter = statsRecv.counter("backend", "backups", "backups_sent")
    val backupsBefore = counter()
    val backupsSeenBefore = backupsSeen.get
    await(client.echo(Echo.Echo.Args("hi")))
    assert(backupsSeen.get == backupsSeenBefore + 1)
    eventually {
      assert(counter() == backupsBefore + 1)
      assertClientTracerMessages(clientMessages)
      assertServerTracerMessages(serverMessages)
    }
  }

  test("Backup requests happen post partitioning in Thriftmux client") {
    // ------------ Setup servers ------------
    // Healthy service
    val service = new Echo.MethodPerEndpoint {
      def echo(msg: String): Future[String] = {
        Future.value(msg)
      }
    }

    // Unhealthy service
    val verySlowService = new Echo.MethodPerEndpoint {
      def echo(msg: String): Future[String] = {
        val response = Future.value(msg)
        response.delayed(200.milliseconds)(DefaultTimer)
      }
    }

    // Setup healthy server
    val healthyServerSR = new InMemoryStatsReceiver()
    val healthyServer: ListeningServer = ThriftMux.server
      .withStatsReceiver(healthyServerSR).withLabel("healthyServer").serveIface(
        new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
        service)

    // Setup unhealthy server number 1
    val unhealthyServerSR1 = new InMemoryStatsReceiver()
    val unhealthyServer1: ListeningServer = ThriftMux.server
      .withStatsReceiver(unhealthyServerSR1).withLabel("unhealthyServer1").serveIface(
        new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
        verySlowService)

    // Setup unhealthy server number 2
    val unhealthyServerSR2 = new InMemoryStatsReceiver()
    val unhealthyServer2: ListeningServer = ThriftMux.server
      .withStatsReceiver(unhealthyServerSR2).withLabel("unhealthyServer2").serveIface(
        new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
        verySlowService)

    val servers: Seq[ListeningServer] = Seq(healthyServer, unhealthyServer1, unhealthyServer2)

    def newAddress(inet: InetSocketAddress, weight: Int): Address = {
      val shardId = inet.getPort

      val md = ZkMetadata.toAddrMetadata(ZkMetadata(Some(shardId)))
      val addr = new Address.Inet(inet, md) {
        override def toString: String = s"Address(${inet.getPort})-($shardId)"
      }

      WeightedAddress(addr, weight)
    }

    def addresses = servers.map { server =>
      val inet = server.boundAddress.asInstanceOf[InetSocketAddress]
      newAddress(inet, 1)
    }

    def shardIds = servers.map { server =>
      val inet = server.boundAddress.asInstanceOf[InetSocketAddress]
      inet.getPort
    }

    // ------------ Setup partitioning strategy and merger ------------
    val echoMerger: ResponseMerger[String] = (successes, failures) =>
      if (successes.nonEmpty) Return(successes.mkString(";"))
      else Throw(failures.head)

    // Fan out the same request to all three servers
    val customPartitioningStrategy = new MethodBuilderCustomStrategy(
      { echo: Echo.Echo.Args =>
        val echoCopy1 = echo.copy()
        val echoCopy2 = echo.copy()
        Future.value(Map(0 -> echo, 1 -> echoCopy1, 2 -> echoCopy2))
      },
      getLogicalPartitionId = {
        case i if i == healthyServer.boundAddress.asInstanceOf[InetSocketAddress].getPort => Seq(0)
        case i if i == unhealthyServer1.boundAddress.asInstanceOf[InetSocketAddress].getPort =>
          Seq(1)
        case i if i == unhealthyServer2.boundAddress.asInstanceOf[InetSocketAddress].getPort =>
          Seq(2)
      },
      Some(echoMerger)
    )

    // ------------ Setup the client ------------
    val statsRecv = new InMemoryStatsReceiver()
    val client = ThriftMux.client
      .withStatsReceiver(statsRecv)
      .withRetryBudget(RetryBudget.Empty)
      .withLabel("backend")
      .methodBuilder(Name.bound(addresses: _*))
      .idempotent(99.percent)
      .withPartitioningStrategy(customPartitioningStrategy)
      .servicePerEndpoint[Echo.ServicePerEndpoint]

    // Send a bunch of requests
    0.until(100).foreach { i =>
      withClue(s"warmup $i") {
        await(client.echo(Echo.Echo.Args("hi")))
      }
    }

    // Get post-request counters
    def clientBackupCounter = statsRecv.counter("backend", "backups", "backups_sent")
    def clientLogicalReqCounter = statsRecv.counter("backend", "logical", "requests")
    def healthyServerReqCounter = healthyServerSR.counter("healthyServer", "requests")
    def unhealthyServer1ReqCounter = unhealthyServerSR1.counter("unhealthyServer1", "requests")
    def unhealthyServer2ReqCounter = unhealthyServerSR2.counter("unhealthyServer2", "requests")

    // Healthy server gets the same number of client logical requests
    eventually {
      assert(healthyServerReqCounter() == clientLogicalReqCounter())
    }

    def backupsSentToUnhealthy1 = unhealthyServer1ReqCounter() - clientLogicalReqCounter()
    def backupsSentToUnhealthy2 = unhealthyServer2ReqCounter() - clientLogicalReqCounter()
    def totalBackupsSentAccordingToServers = backupsSentToUnhealthy1 + backupsSentToUnhealthy2

    eventually {
      assert(totalBackupsSentAccordingToServers == clientBackupCounter())
    }
  }

}
