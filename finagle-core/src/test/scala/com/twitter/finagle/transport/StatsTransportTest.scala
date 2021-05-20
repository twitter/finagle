package com.twitter.finagle.transport

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.stats.{CategorizingExceptionStatsHandler, InMemoryStatsReceiver}
import com.twitter.util.{Await, Future}
import org.scalatest.funsuite.AnyFunSuite

class StatsTransportTest extends AnyFunSuite {

  test("read/write failures") {
    val exc = new Exception("boom")
    val q = new AsyncQueue[Int]
    val sr = new InMemoryStatsReceiver
    val handler = new CategorizingExceptionStatsHandler
    val trans = new StatsTransport(
      new QueueTransport[Int, Int](q, q) {
        override def read(): Future[Int] = Future.exception(exc)
        override def write(i: Int): Future[Unit] = Future.exception(exc)
      },
      handler,
      sr
    )

    val exc0 = intercept[Exception] { Await.result(trans.read()) }
    val exc1 = intercept[Exception] { Await.result(trans.write(10)) }

    assert(exc0 eq exc)
    assert(exc1 eq exc)

    assert(sr.counters(Seq("write", "failures", "java.lang.Exception")) == 1)
    assert(sr.counters(Seq("write", "failures")) == 1)
    assert(sr.counters(Seq("read", "failures", "java.lang.Exception")) == 1)
    assert(sr.counters(Seq("read", "failures")) == 1)
  }
}
