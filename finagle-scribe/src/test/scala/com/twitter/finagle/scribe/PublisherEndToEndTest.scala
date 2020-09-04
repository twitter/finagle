package com.twitter.finagle.scribe

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.thrift.scribe.thriftscala.{LogEntry, ResultCode, Scribe}
import com.twitter.finagle.{ListeningServer, Thrift, liveness}
import com.twitter.util.{Await, Awaitable, Duration, Future}
import java.net.{InetAddress, InetSocketAddress}
import org.scalactic.source.Position
import org.scalatest.Tag
import org.scalatest.funsuite.AnyFunSuite
import scala.collection.mutable.ArrayBuffer

private object PublisherEndToEndTest {

  trait ThriftTestServer {
    val collectedMessages: ArrayBuffer[LogEntry] = new ArrayBuffer[LogEntry]()
    val server: ListeningServer = Thrift.server.serveIface(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      new Scribe.MethodPerEndpoint {
        def log(messages: scala.collection.Seq[LogEntry]): Future[ResultCode] = {
          collectedMessages.append(messages.toSeq: _*) // Scala 2.13 needs the `.toSeq` here.
          Future.value(ResultCode.Ok)
        }
      }
    )
  }
}

/** Test Finagle Thrift client stats are correct and as expected */
class PublisherEndToEndTest extends AnyFunSuite {
  import PublisherEndToEndTest._

  def await[T](a: Awaitable[T], d: Duration = 5.seconds): T = Await.result(a, d)

  // turn off failure detector since we don't need it for these tests.
  override def test(testName: String, testTags: Tag*)(f: => Any)(implicit pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      liveness.sessionFailureDetector.let("none") { f }
    }
  }

  test("publisher metrics") {
    val logEntry = LogEntry(
      category = "scribe",
      message = "Hello, world\n"
    )

    new ThriftTestServer {
      val inMemoryStatsReceiver = new InMemoryStatsReceiver
      val serverInetSocketAddress: InetSocketAddress =
        server.boundAddress.asInstanceOf[InetSocketAddress]
      val publisher: Publisher =
        Publisher.builder
          .withStatsReceiver(inMemoryStatsReceiver)
          .withDest(
            s"inet!${serverInetSocketAddress.getHostName}:${serverInetSocketAddress.getPort}")
          .build("scribe", "foo-client")

      try {
        await(publisher.write(Seq(logEntry)))
        assert(collectedMessages.size == 1)
        assert(collectedMessages.head == logEntry)

        assert(inMemoryStatsReceiver.counter("foo-client", "scribe", "try_later")() == 0)
        assert(inMemoryStatsReceiver.counter("foo-client", "scribe", "ok")() == 1)

        assert(inMemoryStatsReceiver.counter("clnt", "foo-client", "logical", "requests")() == 1)
        assert(inMemoryStatsReceiver.counter("clnt", "foo-client", "logical", "success")() == 1)
        assert(
          inMemoryStatsReceiver
            .stat("clnt", "foo-client", "logical", "request_latency_ms")().map(_.toInt).nonEmpty)

        assert(inMemoryStatsReceiver.counter("clnt", "foo-client", "requests")() == 1)
        assert(inMemoryStatsReceiver.counter("clnt", "foo-client", "success")() == 1)

        assert(inMemoryStatsReceiver.stat("clnt", "foo-client", "retries")().map(_.toInt) == Seq(0))
        assert(
          inMemoryStatsReceiver
            .stat("clnt", "foo-client", "retries", "requeues_per_request")().map(_.toInt) == Seq(0))
        assert(
          inMemoryStatsReceiver.counter("clnt", "foo-client", "retries", "budget_exhausted")() == 0)
        assert(
          inMemoryStatsReceiver.counter("clnt", "foo-client", "retries", "cannot_retry")() == 0)
        assert(inMemoryStatsReceiver.counter("clnt", "foo-client", "retries", "not_open")() == 0)
        assert(
          inMemoryStatsReceiver.counter("clnt", "foo-client", "retries", "request_limit")() == 0)
        assert(inMemoryStatsReceiver.counter("clnt", "foo-client", "retries", "requeues")() == 0)

        // finagle client gauges
        assert(inMemoryStatsReceiver.gauges(Seq("clnt", "foo-client", "logical", "pending"))() == 0)
        assert(inMemoryStatsReceiver.gauges(Seq("clnt", "foo-client", "pending"))() == 0)
        assert(
          inMemoryStatsReceiver.gauges(Seq("clnt", "foo-client", "retries", "budget"))() == 100)
      } finally {
        await(publisher.close())
        await(server.close())
      }
    }
  }
}
