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

    val localSize = statsReceiver.stat("prefix", "dtab", "local", "size")
    val totalSize = statsReceiver.stat("prefix", "dtab", "size")
    assert(localSize() == Nil)
    assert(totalSize() == Nil)
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

    val localSize = statsReceiver.stat("prefix", "dtab", "local", "size")
    val totalSize = statsReceiver.stat("prefix", "dtab", "size")

    assert(localSize() == List(3.0))
    assert(totalSize() == List(3.0))
  }

  test("empty Dtab.limited") {
    val statsReceiver = new InMemoryStatsReceiver
    val service =
      new DtabStatsFilter[Unit, Unit](statsReceiver.scope("prefix")) andThen
        Service.mk[Unit, Unit](_ => Future.Unit)

    Dtab.unwind {
      Dtab.limited = Dtab.empty
      Await.result(service((): Unit))
    }

    val limitedSize = statsReceiver.stat("prefix", "dtab", "limited", "size")
    val totalSize = statsReceiver.stat("prefix", "dtab", "size")
    assert(limitedSize() == Nil)
    assert(totalSize() == Nil)
  }

  test("non-empty Dtab.limited") {
    val statsReceiver = new InMemoryStatsReceiver
    val service =
      new DtabStatsFilter[Unit, Unit](statsReceiver.scope("prefix")) andThen
        Service.mk[Unit, Unit](_ => Future.Unit)

    Dtab.unwind {
      Dtab.limited = Dtab.read("/s=>/foo;/s=>/bar;/s=>/bah")
      Await.result(service((): Unit))
    }

    val limitedSize = statsReceiver.stat("prefix", "dtab", "limited", "size")
    val totalSize = statsReceiver.stat("prefix", "dtab", "size")
    assert(limitedSize() == List(3.0))
    assert(totalSize() == List(3.0))
  }

  test("dtab/size") {
    val statsReceiver = new InMemoryStatsReceiver
    val service =
      new DtabStatsFilter[Unit, Unit](statsReceiver.scope("prefix")) andThen
        Service.mk[Unit, Unit](_ => Future.Unit)

    Dtab.unwind {
      Dtab.local = Dtab.empty
      Await.result(service((): Unit))
    }
    val stat1 = statsReceiver.stat("prefix", "dtab", "size")
    assert(stat1() == Nil)

    Dtab.unwind {
      Dtab.limited = Dtab.read("/s=>/foo;/s=>/bar;/s=>/bah")
      Await.result(service((): Unit))
    }

    val stat2 = statsReceiver.stat("prefix", "dtab", "size")
    assert(stat2() == List(3.0))

    Dtab.unwind {
      Dtab.local = Dtab.read("/s=>/aaa;/s=>/bbb;/s=>/ccc")
      Dtab.limited = Dtab.read("/s=>/ddd;/s=>/eee;/s=>/fff")
      Await.result(service((): Unit))
    }

    val stat3 = statsReceiver.stat("prefix", "dtab", "size")
    assert(stat3() == List(3.0, 6.0))
  }
}
