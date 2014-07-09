package com.twitter.finagle.serverset2

import com.twitter.finagle.{Addr, WeightedSocketAddress}
import com.twitter.util.{Var, Event, Witness}
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
    val epoch = Event[Unit]()

    val stable = Stabilizer(va, epoch)
    val ref = new AtomicReference[Addr]
    stable.changes.register(Witness(ref))

    def assertStabilized(addr: Addr) = assert(ref.get === addr)
    def pulse() = epoch.notify(())
    
    assertStabilized(Addr.Pending)
  }

  test("Additions are reflected immediately; "+
    "removes are reflected after at least one epoch") (new Ctx {

    va() = Addr.Bound(sa1)
    assertStabilized(Addr.Bound(sa1))
    va() = Addr.Bound(sa2)
    assertStabilized(Addr.Bound(sa1, sa2))

    pulse()
    assertStabilized(Addr.Bound(sa1, sa2))
    va() = Addr.Neg
    assertStabilized(Addr.Bound(sa1, sa2))

    pulse()
    assertStabilized(Addr.Bound(sa2))
    
    pulse()
    assertStabilized(Addr.Neg)
  })

  test("Pending resolutions don't tick out successful results") (new Ctx {
    va() = Addr.Bound(sa1)
    assertStabilized(Addr.Bound(sa1))
    va() = Addr.Bound(sa2)
    assertStabilized(Addr.Bound(sa1, sa2))
    
    va() = Addr.Failed(new Exception)
    pulse()
    pulse()
    
    assertStabilized(Addr.Bound(sa1, sa2))
    
    va() = Addr.Pending
    pulse()
    pulse()

    assertStabilized(Addr.Bound(sa1, sa2))
  })


  test("Removes are delayed while failures are observed") (new Ctx {
    va() = Addr.Bound(sa1, sa2)
    assertStabilized(Addr.Bound(sa1, sa2))
    
    pulse()
    assertStabilized(Addr.Bound(sa1, sa2))
    
    va() = Addr.Bound(sa1)
    assertStabilized(Addr.Bound(sa1, sa2))

    pulse()
    assertStabilized(Addr.Bound(sa1, sa2))
    va() = Addr.Failed(new Exception)

    assertStabilized(Addr.Bound(sa1, sa2))

    pulse()
    assertStabilized(Addr.Bound(sa1, sa2))
    
    pulse(); pulse(); pulse()
    assertStabilized(Addr.Bound(sa1, sa2))
    
    va() = Addr.Bound(sa1)
    assertStabilized(Addr.Bound(sa1, sa2))
    
    pulse()
    assertStabilized(Addr.Bound(sa1, sa2))
    pulse()
    assertStabilized(Addr.Bound(sa1))
  })
  
  test("Reflect additions while addrs are unstable") (new Ctx {
    va() = Addr.Bound(sa1, sa2)
    assertStabilized(Addr.Bound(sa1, sa2))


    pulse()
    va() = Addr.Failed(new Exception)
    assertStabilized(Addr.Bound(sa1, sa2))
    
    pulse()
    assertStabilized(Addr.Bound(sa1, sa2))
    
    pulse()
    va() = Addr.Bound(sa3)
    assertStabilized(Addr.Bound(sa1, sa2, sa3))
    va() = Addr.Failed(new Exception)
    pulse()
  })

  test("Merge WeightedSocketAddresses") (new Ctx {
    va() = Addr.Bound(wsa1, sa2, sa3)
    assertStabilized(Addr.Bound(wsa1, sa2, sa3))

    pulse()
    va() = Addr.Bound(wsa2, sa2)
    assertStabilized(Addr.Bound(wsa2, sa2, sa3))

    pulse()
    assertStabilized(Addr.Bound(wsa2, sa2, sa3))

    pulse()
    assertStabilized(Addr.Bound(wsa2, sa2))

    pulse()
    va() = Addr.Bound(wsa2, wsa3)
    assertStabilized(Addr.Bound(wsa2, wsa3))

    pulse()
    assertStabilized(Addr.Bound(wsa2, wsa3))
  })
}
