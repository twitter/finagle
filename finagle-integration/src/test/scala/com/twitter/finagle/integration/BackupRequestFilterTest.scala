package com.twitter.finagle.integration

import com.twitter.conversions.DurationOps._
import com.twitter.conversions.PercentOps._
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
import com.twitter.finagle.Service
import com.twitter.finagle.ThriftMux
import com.twitter.finagle.http
import com.twitter.util.Await
import com.twitter.util.Future
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

}
