package com.twitter.finagle.serverset2

import com.twitter.conversions.time._
import com.twitter.finagle.{Addr, WeightedSocketAddress}
import com.twitter.util.{Var, Event, Witness, Time}
import java.net.SocketAddress
import java.util.concurrent.atomic.AtomicReference
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StabilizerTest extends FunSuite {
  class Ctx {
    val sa1 = new SocketAddress{ override def toString() = "sa1" }
    val sa2 = new SocketAddress{ override def toString() = "sa2" }
    val sa3 = new SocketAddress{ override def toString() = "sa3" }
    val wsa1 = WeightedSocketAddress(sa1, 1D)
    val wsa2 = WeightedSocketAddress(sa1, 2D)
    val wsa3 = WeightedSocketAddress(sa2, 2D)

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

    setVa(Addr.Bound(sa1))
    assertStabilized(Addr.Bound(sa1))
    setVa(Addr.Bound(sa2))
    assertStabilized(Addr.Bound(sa1, sa2))

    pulse()
    assertStabilized(Addr.Bound(sa1, sa2))
    setVa(Addr.Neg)
    assertStabilized(Addr.Bound(sa1, sa2))

    pulse()
    assertStabilized(Addr.Bound(sa2))

    pulse()
    assertStabilized(Addr.Neg)
  })

  test("Pending resolutions don't tick out successful results") (new Ctx {
    setVa(Addr.Bound(sa1))
    assertStabilized(Addr.Bound(sa1))
    setVa(Addr.Bound(sa2))
    assertStabilized(Addr.Bound(sa1, sa2))

    setVa(Addr.Failed(new Exception))
    pulse()
    pulse()

    assertStabilized(Addr.Bound(sa1, sa2))

    setVa(Addr.Pending)
    pulse()
    pulse()

    assertStabilized(Addr.Bound(sa1, sa2))
  })


  test("Removes are delayed while failures are observed") (new Ctx {
    setVa(Addr.Bound(sa1, sa2))
    assertStabilized(Addr.Bound(sa1, sa2))

    pulse()
    assertStabilized(Addr.Bound(sa1, sa2))

    setVa(Addr.Bound(sa1))
    assertStabilized(Addr.Bound(sa1, sa2))

    pulse()
    assertStabilized(Addr.Bound(sa1, sa2))
    setVa(Addr.Failed(new Exception))

    assertStabilized(Addr.Bound(sa1, sa2))

    pulse()
    assertStabilized(Addr.Bound(sa1, sa2))

    pulse(); pulse(); pulse()
    assertStabilized(Addr.Bound(sa1, sa2))

    setVa(Addr.Bound(sa1))
    assertStabilized(Addr.Bound(sa1, sa2))

    pulse()
    assertStabilized(Addr.Bound(sa1, sa2))
    pulse()
    assertStabilized(Addr.Bound(sa1))
  })

  test("Reflect additions while addrs are unstable") (new Ctx {
    setVa(Addr.Bound(sa1, sa2))
    assertStabilized(Addr.Bound(sa1, sa2))


    pulse()
    setVa(Addr.Failed(new Exception))
    assertStabilized(Addr.Bound(sa1, sa2))

    pulse()
    assertStabilized(Addr.Bound(sa1, sa2))

    pulse()
    setVa(Addr.Bound(sa3))
    assertStabilized(Addr.Bound(sa1, sa2, sa3))
    setVa(Addr.Failed(new Exception))
    pulse()
  })

  test("Merge WeightedSocketAddresses") (new Ctx {
    setVa(Addr.Bound(wsa1, sa2, sa3))
    assertStabilized(Addr.Bound(wsa1, sa2, sa3))

    pulse()
    setVa(Addr.Bound(wsa2, sa2))
    assertStabilized(Addr.Bound(wsa2, sa2, sa3))

    pulse()
    assertStabilized(Addr.Bound(wsa2, sa2, sa3))

    pulse()
    assertStabilized(Addr.Bound(wsa2, sa2))

    pulse()
    setVa(Addr.Bound(wsa2, wsa3))
    assertStabilized(Addr.Bound(wsa2, wsa3))

    pulse()
    assertStabilized(Addr.Bound(wsa2, wsa3))
  })

  test("Adds and removes are batched by batchEpoch") (new Ctx {
    Time.withCurrentTimeFrozen { timeControl =>
      timeControl.advance(30.seconds)
      // update requires batchEpoch.notify
      va() = Addr.Bound(sa1)
      pulse()
      assertBatchStabilized(Addr.Bound(sa1))

      // adds are held until batchEpoch.notify
      va() = Addr.Bound(sa1, sa2)
      assertBatchStabilized(Addr.Bound(sa1))
      timeControl.advance(30.seconds)
      batchEvent.notify(())
      assertBatchStabilized(Addr.Bound(sa1, sa2))

      // removals are held until both removalEpoch and batchEpoch notify
      va() = Addr.Bound(sa1, sa3)
      assertBatchStabilized(Addr.Bound(sa1, sa2))
      timeControl.advance(30.seconds)
      batchEvent.notify(())
      // no pulse, no removals yet
      assertBatchStabilized(Addr.Bound(sa1, sa2, sa3))
      removalEvent.notify(())
      removalEvent.notify(())
      timeControl.advance(30.seconds)
      batchEvent.notify(())
      assertBatchStabilized(Addr.Bound(sa1, sa3))

      // multiple changes are batched into one update
      va() = Addr.Bound(wsa1, sa3)
      assertBatchStabilized(Addr.Bound(sa1, sa3))
      removalEvent.notify(())
      va() = Addr.Bound(wsa1, sa2, sa3)
      removalEvent.notify(())
      assertBatchStabilized(Addr.Bound(sa1, sa3))
      timeControl.advance(30.seconds)
      batchEvent.notify(())
      assertBatchStabilized(Addr.Bound(wsa1, sa2, sa3))
    }
  })

  test("Adds are published immediately when >1 epoch has passed since last update") (new Ctx {
    Time.withCurrentTimeFrozen { timeControl =>
      timeControl.advance(30.seconds)
      va() = Addr.Bound(sa1)
      assertBatchStabilized(Addr.Bound(sa1))

      va() = Addr.Bound(sa1, sa2)
      assertBatchStabilized(Addr.Bound(sa1))
      timeControl.advance(30.seconds)
      batchEvent.notify(())
      assertBatchStabilized(Addr.Bound(sa1, sa2))

      timeControl.advance(30.seconds)
      va() = Addr.Bound(sa1, sa2, sa3)
      assertBatchStabilized(Addr.Bound(sa1, sa2, sa3))
    }
  })

  test("First update does not wait for epoch to turn") (new Ctx {
    Time.withCurrentTimeFrozen { timeControl =>
      timeControl.advance(1.seconds)
      va() = Addr.Bound(sa1)
      assertBatchStabilized(Addr.Bound(sa1))
    }
  })
}
