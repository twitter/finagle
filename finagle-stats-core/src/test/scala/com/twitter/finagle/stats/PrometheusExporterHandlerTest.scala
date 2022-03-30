package com.twitter.finagle.stats

import com.twitter.conversions.DurationOps._
import com.twitter.util.Await
import com.twitter.util.Awaitable
import java.util.concurrent.atomic.AtomicBoolean
import org.scalatest.funsuite.AnyFunSuite

class PrometheusExporterHandlerTest extends AnyFunSuite {

  def await[T](t: Awaitable[T]): T = Await.result(t, 5.seconds)

  test("close() logs metrics data when logOnShutdown is enabled") {
    logOnShutdown.let(true) {
      val logSeen = new AtomicBoolean()
      val exporter = new PrometheusExporterHandler(new TestMetricsView(Seq(), Seq(), Seq())) {
        override protected def doLog(): Unit = {
          logSeen.set(true)
        }
      }
      val f = exporter.close()

      await(f)
      assert(logSeen.get)
    }
  }

  test("close() does not log metric data when logOnShutdown is disabled") {
    logOnShutdown.let(false) {
      val logSeen = new AtomicBoolean()
      val exporter = new PrometheusExporterHandler(new TestMetricsView(Seq(), Seq(), Seq())) {
        override protected def doLog(): Unit = {
          logSeen.set(true)
        }
      }
      val f = exporter.close()

      await(f)
      assert(!logSeen.get)
    }
  }
}
