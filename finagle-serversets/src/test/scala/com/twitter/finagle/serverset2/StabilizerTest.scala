package com.twitter.finagle.serverset2

import com.twitter.conversions.time._
import com.twitter.finagle.{Addr, Address}
import com.twitter.finagle.addr.WeightedAddress
import com.twitter.util.{Var, Event, Witness, Time}
import java.util.concurrent.atomic.AtomicReference
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StabilizerTest extends FunSuite {
  class Ctx {
    val addr1 = Address(1)
    val addr2 = Address(2)
    val addr3 = Address(3)
    val waddr1 = WeightedAddress(addr1, 1D)
    val waddr2 = WeightedAddress(addr1, 2D)
    val waddr3 = WeightedAddress(addr2, 2D)

    val va = Var[Addr](Addr.Pending)
    val removalEvent = Event[Unit]()
    val batchEvent = Event[Unit]()
    val removalEpoch = new Epoch(removalEvent, -1.seconds)
    val batchEpoch = new Epoch(batchEvent, -1.seconds)

    val slowBatchEpoch = new Epoch(batchEvent, 10.seconds)

    val stable = Stabilizer(va, removalEpoch, batchEpoch)
    val ref = new AtomicReference[Addr]
    stable.changes.register(Witness(ref))

    val batchStable = Stabilizer(va, removalEpoch, slowBatchEpoch)
    val ref2 = new AtomicReference[Addr]
    batchStable.changes.register(Witness(ref2))

    def assertStabilized(addr: Addr) = assert(ref.get == addr)
    def assertBatchStabilized(addr: Addr) = assert(ref2.get == addr)
    def pulse() = {
      removalEvent.notify(())
      batchEvent.notify(())
    }
    def setVa(a: Addr) = {
      va() = a
      batchEvent.notify(())
    }

    assertStabilized(Addr.Pending)
  }

  test("Additions are reflected immediately; "+
    "removes are reflected after at least one removalEpoch") (new Ctx {

    setVa(Addr.Bound(addr1))
    assertStabilized(Addr.Bound(addr1))
    setVa(Addr.Bound(addr2))
    assertStabilized(Addr.Bound(addr1, addr2))

    pulse()
    assertStabilized(Addr.Bound(addr1, addr2))
    setVa(Addr.Neg)
    assertStabilized(Addr.Bound(addr1, addr2))

    pulse()
    assertStabilized(Addr.Bound(addr2))

    pulse()
    assertStabilized(Addr.Neg)
  })

  test("Pending resolutions don't tick out successful results") (new Ctx {
    setVa(Addr.Bound(addr1))
    assertStabilized(Addr.Bound(addr1))
    setVa(Addr.Bound(addr2))
    assertStabilized(Addr.Bound(addr1, addr2))

    setVa(Addr.Failed(new Exception))
    pulse()
    pulse()

    assertStabilized(Addr.Bound(addr1, addr2))

    setVa(Addr.Pending)
    pulse()
    pulse()

    assertStabilized(Addr.Bound(addr1, addr2))
  })


  test("Removes are delayed while failures are observed") (new Ctx {
    setVa(Addr.Bound(addr1, addr2))
    assertStabilized(Addr.Bound(addr1, addr2))

    pulse()
    assertStabilized(Addr.Bound(addr1, addr2))

    setVa(Addr.Bound(addr1))
    assertStabilized(Addr.Bound(addr1, addr2))

    pulse()
    assertStabilized(Addr.Bound(addr1, addr2))
    setVa(Addr.Failed(new Exception))

    assertStabilized(Addr.Bound(addr1, addr2))

    pulse()
    assertStabilized(Addr.Bound(addr1, addr2))

    pulse(); pulse(); pulse()
    assertStabilized(Addr.Bound(addr1, addr2))

    setVa(Addr.Bound(addr1))
    assertStabilized(Addr.Bound(addr1, addr2))

    pulse()
    assertStabilized(Addr.Bound(addr1, addr2))
    pulse()
    assertStabilized(Addr.Bound(addr1))
  })

  test("Removes are delayed while failures are observed on empty serversets") (new Ctx {
    setVa(Addr.Neg)
    assertStabilized(Addr.Neg)

    pulse()
    setVa(Addr.Failed(new Exception))

    assertStabilized(Addr.Neg)

    pulse()
    assertStabilized(Addr.Neg)

    pulse(); pulse(); pulse()
    assertStabilized(Addr.Neg)

    setVa(Addr.Bound(addr1))
    pulse()
    assertStabilized(Addr.Bound(addr1))
  })

  test("Reflect additions while addrs are unstable") (new Ctx {
    setVa(Addr.Bound(addr1, addr2))
    assertStabilized(Addr.Bound(addr1, addr2))


    pulse()
    setVa(Addr.Failed(new Exception))
    assertStabilized(Addr.Bound(addr1, addr2))

    pulse()
    assertStabilized(Addr.Bound(addr1, addr2))

    pulse()
    setVa(Addr.Bound(addr3))
    assertStabilized(Addr.Bound(addr1, addr2, addr3))
    setVa(Addr.Failed(new Exception))
    pulse()
  })

  test("Merge WeightedSocketAddresses") (new Ctx {
    setVa(Addr.Bound(waddr1, addr2, addr3))
    assertStabilized(Addr.Bound(waddr1, addr2, addr3))

    pulse()
    setVa(Addr.Bound(waddr2, addr2))
    assertStabilized(Addr.Bound(waddr2, addr2, addr3))

    pulse()
    assertStabilized(Addr.Bound(waddr2, addr2, addr3))

    pulse()
    assertStabilized(Addr.Bound(waddr2, addr2))

    pulse()
    setVa(Addr.Bound(waddr2, waddr3))
    assertStabilized(Addr.Bound(waddr2, waddr3))

    pulse()
    assertStabilized(Addr.Bound(waddr2, waddr3))
  })

  test("Adds and removes are batched by batchEpoch") (new Ctx {
    Time.withCurrentTimeFrozen { timeControl =>
      timeControl.advance(30.seconds)
      // update requires batchEpoch.notify
      va() = Addr.Bound(addr1)
      pulse()
      assertBatchStabilized(Addr.Bound(addr1))

      // adds are held until batchEpoch.notify
      va() = Addr.Bound(addr1, addr2)
      assertBatchStabilized(Addr.Bound(addr1))
      timeControl.advance(30.seconds)
      batchEvent.notify(())
      assertBatchStabilized(Addr.Bound(addr1, addr2))

      // removals are held until both removalEpoch and batchEpoch notify
      va() = Addr.Bound(addr1, addr3)
      assertBatchStabilized(Addr.Bound(addr1, addr2))
      timeControl.advance(30.seconds)
      batchEvent.notify(())
      // no pulse, no removals yet
      assertBatchStabilized(Addr.Bound(addr1, addr2, addr3))
      removalEvent.notify(())
      removalEvent.notify(())
      timeControl.advance(30.seconds)
      batchEvent.notify(())
      assertBatchStabilized(Addr.Bound(addr1, addr3))

      // multiple changes are batched into one update
      va() = Addr.Bound(waddr1, addr3)
      assertBatchStabilized(Addr.Bound(addr1, addr3))
      removalEvent.notify(())
      va() = Addr.Bound(waddr1, addr2, addr3)
      removalEvent.notify(())
      assertBatchStabilized(Addr.Bound(addr1, addr3))
      timeControl.advance(30.seconds)
      batchEvent.notify(())
      assertBatchStabilized(Addr.Bound(waddr1, addr2, addr3))
    }
  })

  test("Adds are published immediately when >1 epoch has passed since last update") (new Ctx {
    Time.withCurrentTimeFrozen { timeControl =>
      timeControl.advance(30.seconds)
      va() = Addr.Bound(addr1)
      assertBatchStabilized(Addr.Bound(addr1))

      va() = Addr.Bound(addr1, addr2)
      assertBatchStabilized(Addr.Bound(addr1))
      timeControl.advance(30.seconds)
      batchEvent.notify(())
      assertBatchStabilized(Addr.Bound(addr1, addr2))

      timeControl.advance(30.seconds)
      va() = Addr.Bound(addr1, addr2, addr3)
      assertBatchStabilized(Addr.Bound(addr1, addr2, addr3))
    }
  })

  test("First update does not wait for epoch to turn") (new Ctx {
    Time.withCurrentTimeFrozen { timeControl =>
      timeControl.advance(1.seconds)
      va() = Addr.Bound(addr1)
      assertBatchStabilized(Addr.Bound(addr1))
    }
  })
}
