package com.twitter.finagle.mysql

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.stats.{
  Counter,
  Gauge,
  InMemoryStatsReceiver,
  MetricBuilder,
  Stat,
  StatsReceiverProxy
}
import com.twitter.util.{Await, Awaitable, Future}
import java.util.concurrent.atomic.LongAdder
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite

class MetricsTest extends AnyFunSuite {

  private[this] def await[T](t: Awaitable[T]): T = Await.result(t, 5.seconds)

  // MetricsStatsReceiver with metric call counts exposed for testing.
  class VisibleMetricsStatsReceiver(val self: InMemoryStatsReceiver) extends StatsReceiverProxy {
    val counterCounter = new LongAdder()
    val statCounter = new LongAdder()
    val gaugeCounter = new LongAdder()

    override def counter(metricBuilder: MetricBuilder): Counter = {
      counterCounter.increment()
      super.counter(metricBuilder)
    }

    override def stat(metricBuilder: MetricBuilder): Stat = {
      statCounter.increment()
      super.stat(metricBuilder)
    }

    override def addGauge(metricBuilder: MetricBuilder)(f: => Float): Gauge = {
      gaugeCounter.increment()
      super.addGauge(metricBuilder)(f)
    }
  }

  // See https://github.com/twitter/finagle/issues/712.
  test("transactions do not create new CursorStats") {
    val service = new MockService()
    val factory = spy(new MockServiceFactory(service))
    val sr = new VisibleMetricsStatsReceiver(new InMemoryStatsReceiver)
    val client = Client(factory, sr, supportUnsigned = false)

    val runTransactions: Seq[Future[ResultSet]] =
      (1 to 50).map { _ =>
        client.transaction {
          _.read("select * from foo")
        }
      }

    await(Future.collect(runTransactions))

    assert(sr.counterCounter.sum() == 0)
    assert(sr.statCounter.sum() == 0)
    assert(sr.gaugeCounter.sum() == 0)
  }

  test("new CursoredStatements create new CursorStats") {
    val service = new MockService()
    val factory = spy(new MockServiceFactory(service))
    val sr = new VisibleMetricsStatsReceiver(new InMemoryStatsReceiver)
    val client = Client(factory, sr, supportUnsigned = false)

    val query = "select * from `foo` where `bar` = ?"
    val cursoredStatement = client.cursor(query)
    await(cursoredStatement(1, "baz")(r => r))

    assert(sr.counterCounter.sum() == 2)
    assert(sr.statCounter.sum() == 3)
    assert(sr.gaugeCounter.sum() == 0)

    val query2 = "select * from `foo` where `foobar` = ?"
    val cursoredStatement2 = client.cursor(query2)
    await(cursoredStatement(1, "qux")(r => r))

    assert(sr.counterCounter.sum() == 4)
    assert(sr.statCounter.sum() == 6)
    assert(sr.gaugeCounter.sum() == 0)
  }

  test("reusing a CursoredStatement does not create new CursorStats") {
    val service = new MockService()
    val factory = spy(new MockServiceFactory(service))
    val sr = new VisibleMetricsStatsReceiver(new InMemoryStatsReceiver)
    val client = Client(factory, sr, supportUnsigned = false)

    val query = "select * from `foo` where `bar` = ?"
    val cursoredStatement = client.cursor(query)
    await(cursoredStatement(1, "baz")(r => r))

    assert(sr.counterCounter.sum() == 2)
    assert(sr.statCounter.sum() == 3)
    assert(sr.gaugeCounter.sum() == 0)

    await(cursoredStatement(1, "qux")(r => r))

    assert(sr.counterCounter.sum() == 2)
    assert(sr.statCounter.sum() == 3)
    assert(sr.gaugeCounter.sum() == 0)
  }
}
