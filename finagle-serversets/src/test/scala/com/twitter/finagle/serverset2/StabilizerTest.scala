package com.twitter.finagle.serverset2

import com.twitter.finagle.Addr
import com.twitter.util.{Var, Event, Witness}
import java.net.SocketAddress
import java.util.concurrent.atomic.AtomicReference
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StabilizerTest extends FunSuite {
  class Ctx {
    val sa1, sa2, sa3 = new SocketAddress{}
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
}
