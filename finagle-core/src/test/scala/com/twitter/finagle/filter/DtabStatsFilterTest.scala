package com.twitter.finagle.filter

import org.scalatestplus.junit.AssertionsForJUnit
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.{Dtab, Service}
import com.twitter.util.{Await, Future}
import org.scalatest.funsuite.AnyFunSuite

class DtabStatsFilterTest extends AnyFunSuite with AssertionsForJUnit {

  test("empty Dtab.local") {
    val statsReceiver = new InMemoryStatsReceiver
    val service =
      new DtabStatsFilter[Unit, Unit](statsReceiver.scope("prefix")) andThen
        Service.mk[Unit, Unit](_ => Future.Unit)

    Dtab.unwind {
      Dtab.local = Dtab.empty
      Await.result(service((): Unit))
    }

    val stat = statsReceiver.stat("prefix", "dtab", "size")
    assert(stat() == Nil)
  }

  test("non-empty Dtab.local") {
    val statsReceiver = new InMemoryStatsReceiver
    val service =
      new DtabStatsFilter[Unit, Unit](statsReceiver.scope("prefix")) andThen
        Service.mk[Unit, Unit](_ => Future.Unit)

    Dtab.unwind {
      Dtab.local = Dtab.read("/s=>/foo;/s=>/bar;/s=>/bah")
      Await.result(service((): Unit))
    }

    val stat = statsReceiver.stat("prefix", "dtab", "size")
    assert(stat() == List(3.0))
  }
}
