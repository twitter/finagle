package com.twitter.finagle.filter

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.{Dtab, Service}
import com.twitter.util.{Await, Future}

@RunWith(classOf[JUnitRunner])
class DtabStatsFilterTest extends FunSuite with AssertionsForJUnit {

  test("empty Dtab.local") {
    val statsReceiver = new InMemoryStatsReceiver
    val service =
      new DtabStatsFilter[Unit, Unit](statsReceiver.scope("prefix")) andThen
      Service.mk[Unit, Unit](_ => Future.Unit)

    Dtab.unwind {
      Dtab.local = Dtab.empty
      Await.result(service())
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
      Await.result(service())
    }

    val stat = statsReceiver.stat("prefix", "dtab", "size")
    assert(stat() == List(3.0))
  }
}
