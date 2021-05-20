package com.twitter.finagle.serverset2

import com.twitter.finagle.addr.WeightedAddress
import com.twitter.finagle.partitioning.zk.ZkMetadata
import com.twitter.finagle.{Addr, Address}
import com.twitter.util._
import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.atomic.AtomicReference
import org.scalatest.funsuite.FixtureAnyFunSuite

object StabilizerTest {

  def newAddress(port: Int, shard: Option[Int] = None): Address = {
    val inet = new InetSocketAddress(InetAddress.getLoopbackAddress, port)
    val md = shard match {
      case Some(_) => ZkMetadata.toAddrMetadata(ZkMetadata(shard))
      case None => Addr.Metadata.empty
    }
    new Address.Inet(inet, md) {
      override def toString: String = {
        shard match {
          case Some(id) => s"Address($port)-($id)"
          case None => s"Address($port)"
        }
      }
    }
  }

  // Note, all these Addresses are defs to test against structural
  // equality rather than reference equality

  def addr1: Address = newAddress(1)
  def addr2: Address = newAddress(2)
  def addr3: Address = newAddress(3)
  def addr4: Address = newAddress(4)

  // Note, weights are stored as metadata in the
  // original addrs.
  def addr1w1: Address = WeightedAddress(addr1, 1d)
  def addr1w2: Address = WeightedAddress(addr1, 2d)
  def addr2w2: Address = WeightedAddress(addr2, 2d)

  def shard1: Address = newAddress(1, Some(1))
  def shard2: Address = newAddress(2, Some(2))
  def shard3: Address = newAddress(3, Some(3))
  def shard4: Address = newAddress(4, Some(4))
}

class StabilizerTest extends FixtureAnyFunSuite {
  import StabilizerTest._

  // The period is irrelevant for most tests since we pulse the
  // event manually. However, we parameterize it so that we
  // can observe the special case of Duration(0).
  case class FixtureParam(period: Duration = Duration.Top) {
    val va = Var[Addr](Addr.Pending)
    val event = Event[Unit]()

    private val ref: AtomicReference[Addr] = {
      val epoch = new Epoch(event, period)
      val stabilized: Var[Addr] = Stabilizer(va, epoch)
      val r = new AtomicReference[Addr]
      stabilized.changes.register(Witness(r))
      r
    }

    def pulse(): Unit = event.notify(())
    def addrEquals(addr: Addr): Unit = assert(addr == ref.get)
  }

  def withFixture(test: OneArgTest) = test(FixtureParam())

  test("propagate Addr.Neg and Addr.Pending immediately") { ctx =>
    import ctx._

    va() = Addr.Bound(addr1)
    addrEquals(Addr.Bound(addr1))

    va() = Addr.Neg
    addrEquals(Addr.Neg)

    va() = Addr.Pending
    addrEquals(Addr.Pending)
  }

  test("bound is cached in the presence of failures") { ctx =>
    import ctx._

    // we propagate Addr.Neg when there is no previously bound address.
    va() = Addr.Failed(new Exception)
    addrEquals(Addr.Neg)

    // The stream of events are: [Bound, Bound, Failure].
    // We want to propagate the most recently seen bound.

    va() = Addr.Bound(addr1, addr2)
    addrEquals(Addr.Bound(addr1, addr2))

    va() = Addr.Bound(addr1, addr2, addr3)
    addrEquals(Addr.Bound(addr1, addr2))

    va() = Addr.Failed(new Exception)
    addrEquals(Addr.Bound(addr1, addr2))

    // we are pinned to the last bound addr so long
    // as we don't receive anymore updates.
    (0 to 10).foreach { _ =>
      pulse()
      addrEquals(Addr.Bound(addr1, addr2, addr3))
    }

    va() = Addr.Bound(addr1, addr2, addr3, addr4)
    pulse()
    addrEquals(Addr.Bound(addr1, addr2, addr3, addr4))
  }

  test("First update does not wait for epoch to turn") { ctx =>
    import ctx._

    va() = Addr.Bound(addr1)
    addrEquals(Addr.Bound(addr1))
  }

  test("adds are buffered for one epoch") { ctx =>
    import ctx._

    // first add gets flushed immediately
    va() = Addr.Bound(addr1, addr2)
    addrEquals(Addr.Bound(addr1, addr2))

    // `addr3` is added
    va() = Addr.Bound(addr1, addr2, addr3)
    addrEquals(Addr.Bound(addr1, addr2))

    pulse()
    addrEquals(Addr.Bound(addr1, addr2, addr3))
  }

  test("removes are buffered for two epochs") { ctx =>
    import ctx._

    va() = Addr.Bound(addr1, addr2)
    addrEquals(Addr.Bound(addr1, addr2))

    va() = Addr.Bound(addr1)
    addrEquals(Addr.Bound(addr1, addr2))

    pulse()
    addrEquals(Addr.Bound(addr1, addr2))

    pulse()
    addrEquals(Addr.Bound(addr1))
  }

  test("removal with addition") { ctx =>
    import ctx._

    va() = Addr.Bound(addr1, addr2)
    addrEquals(Addr.Bound(addr1, addr2))

    // `addr2` is removed; `addr3` is added
    va() = Addr.Bound(addr1, addr3)
    addrEquals(Addr.Bound(addr1, addr2))

    pulse()
    addrEquals(Addr.Bound(addr1, addr2, addr3))

    pulse()
    addrEquals(Addr.Bound(addr1, addr3))
  }

  test("multiple updates") { ctx =>
    import ctx._

    va() = Addr.Bound(addr1, addr2)
    addrEquals(Addr.Bound(addr1, addr2))

    va() = Addr.Bound(addr1, addr3)
    addrEquals(Addr.Bound(addr1, addr2))

    pulse()
    addrEquals(Addr.Bound(addr1, addr2, addr3))

    va() = Addr.Bound(addr1)
    addrEquals(Addr.Bound(addr1, addr2, addr3))

    pulse()
    addrEquals(Addr.Bound(addr1, addr3))

    pulse()
    addrEquals(Addr.Bound(addr1))
  }

  test("flappy hosts") { ctx =>
    import ctx._

    va() = Addr.Bound(addr1, addr2)
    addrEquals(Addr.Bound(addr1, addr2))

    // `addr2` went away momentarily – we want
    // to avoid this transient state.
    va() = Addr.Bound(addr1)
    addrEquals(Addr.Bound(addr1, addr2))

    pulse()
    addrEquals(Addr.Bound(addr1, addr2))

    // `addr2` comes back before second pulse
    va() = Addr.Bound(addr1, addr2)
    addrEquals(Addr.Bound(addr1, addr2))

    (0 to 100).foreach { _ =>
      pulse()
      addrEquals(Addr.Bound(addr1, addr2))
    }
  }

  test("transient hosts are published and then cleaned up in the next epoch") { ctx =>
    import ctx._

    va() = Addr.Bound(addr1)
    addrEquals(Addr.Bound(addr1)) // published immediately

    // we are still in the same epoch and add2 shows up briefly and leaves
    va() = Addr.Bound(addr1, addr2) // addr2 joins the list
    addrEquals(Addr.Bound(addr1))

    va() = Addr.Bound(addr1, addr3) // addr2 leaves right away (addr3 joins)
    addrEquals(Addr.Bound(addr1))

    pulse()
    addrEquals(Addr.Bound(addr1, addr2, addr3))

    // addr2 goes away with the next pulse
    pulse()
    addrEquals(Addr.Bound(addr1, addr3))
    pulse()
    addrEquals(Addr.Bound(addr1, addr3))
  }

  test("Pending resolutions don't tick out successful results") { ctx =>
    import ctx._

    va() = Addr.Bound(addr1)
    addrEquals(Addr.Bound(addr1))
    va() = Addr.Bound(addr2)
    // removal will take two pulses to flush
    pulse()
    addrEquals(Addr.Bound(addr1, addr2))
    pulse()
    addrEquals(Addr.Bound(addr2))

    va() = Addr.Failed(new Exception)
    pulse()
    addrEquals(Addr.Bound(addr2))
    pulse()
    addrEquals(Addr.Bound(addr2))

    // addr3 shows up momentarily, followed by Pending response. The addresses will be
    // buffered as usual.
    va() = Addr.Bound(addr3)
    addrEquals(Addr.Bound(addr2))

    va() = Addr.Pending
    addrEquals(Addr.Bound(addr2))
    pulse()
    addrEquals(Addr.Bound(addr2, addr3))
    pulse()
    addrEquals(Addr.Bound(addr3))

    va() = Addr.Bound(addr1, addr2, addr3)
    addrEquals(Addr.Bound(addr3))
    pulse()
    addrEquals(Addr.Bound(addr1, addr2, addr3))
    pulse()
    addrEquals(Addr.Bound(addr1, addr2, addr3))
  }

  test("Removes are swallowed and epochs continue to promote addresses") { ctx =>
    import ctx._

    va() = Addr.Bound(addr1, addr2)
    addrEquals(Addr.Bound(addr1, addr2))

    pulse()
    addrEquals(Addr.Bound(addr1, addr2))

    va() = Addr.Bound(addr1)
    addrEquals(Addr.Bound(addr1, addr2))

    va() = Addr.Failed(new Exception)
    addrEquals(Addr.Bound(addr1, addr2))

    pulse()
    addrEquals(Addr.Bound(addr1, addr2))

    pulse()
    addrEquals(Addr.Bound(addr1))

    va() = Addr.Bound(addr1, addr3)
    addrEquals(Addr.Bound(addr1))
    pulse()
    addrEquals(Addr.Bound(addr1, addr3))
    pulse()
    addrEquals(Addr.Bound(addr1, addr3))
  }

  test("Removes are delayed while failures are observed on empty serversets") { ctx =>
    import ctx._

    va() = Addr.Neg
    addrEquals(Addr.Neg)

    pulse()
    va() = Addr.Failed(new Exception)

    addrEquals(Addr.Neg)

    pulse()
    addrEquals(Addr.Neg)

    pulse(); pulse(); pulse()
    addrEquals(Addr.Neg)

    va() = Addr.Bound(addr1)
    pulse()
    addrEquals(Addr.Bound(addr1))
  }

  test("Reflect updates while addrs are unstable") { ctx =>
    import ctx._

    va() = Addr.Bound(addr1, addr2)
    addrEquals(Addr.Bound(addr1, addr2))

    pulse()
    addrEquals(Addr.Bound(addr1, addr2))

    va() = Addr.Failed(new Exception)
    addrEquals(Addr.Bound(addr1, addr2))

    pulse()
    addrEquals(Addr.Bound(addr1, addr2))

    pulse()
    addrEquals(Addr.Bound(addr1, addr2))

    va() = Addr.Bound(addr3)
    addrEquals(Addr.Bound(addr1, addr2))

    va() = Addr.Failed(new Exception)
    addrEquals(Addr.Bound(addr1, addr2))

    pulse()
    addrEquals(Addr.Bound(addr1, addr2, addr3))

    pulse()
    addrEquals(Addr.Bound(addr3))

    pulse()
    addrEquals(Addr.Bound(addr3))

    va() = Addr.Bound(addr2)
    addrEquals(Addr.Bound(addr3))

    pulse()
    addrEquals(Addr.Bound(addr2, addr3))

    pulse()
    addrEquals(Addr.Bound(addr2))

    pulse()
    addrEquals(Addr.Bound(addr2))
  }

  test("merge with weights") { ctx =>
    import ctx._

    va() = Addr.Bound(addr1w1, addr2, addr3)
    addrEquals(Addr.Bound(addr1w1, addr2, addr3))
    pulse()
    addrEquals(Addr.Bound(addr1w1, addr2, addr3))

    va() = Addr.Bound(addr1w2, addr2)
    addrEquals(Addr.Bound(addr1w1, addr2, addr3))

    // pulse twice to flush the removal
    pulse()
    addrEquals(Addr.Bound(addr1w2, addr2, addr3)) // weight change got reflected
    pulse()
    addrEquals(Addr.Bound(addr1w2, addr2))
    pulse(); pulse()
    addrEquals(Addr.Bound(addr1w2, addr2))

    va() = Addr.Bound(addr1w2, addr2w2)
    addrEquals(Addr.Bound(addr1w2, addr2))
    pulse()
    addrEquals(Addr.Bound(addr1w2, addr2w2))
    pulse()
    addrEquals(Addr.Bound(addr1w2, addr2w2))
  }

  test("merge with shard ids") { ctx =>
    import ctx._

    va() = Addr.Bound(shard1, shard2, shard3, shard4)
    addrEquals(Addr.Bound(shard1, shard2, shard3, shard4))

    // shard4 is removed and comes back with a new address
    val newShard4 = newAddress(5, Some(4))
    va() = Addr.Bound(shard1, shard2, shard3, newShard4)
    addrEquals(Addr.Bound(shard1, shard2, shard3, shard4))
    pulse()
    addrEquals(Addr.Bound(shard1, shard2, shard3, newShard4))
    pulse()
    addrEquals(Addr.Bound(shard1, shard2, shard3, newShard4))
  }

  test("continuous restart of shards: one restart per epoch") { ctx =>
    import ctx._

    val initShards = Seq(shard1, shard2, shard3, shard4)
    va() = Addr.Bound(initShards.toSet)
    addrEquals(Addr.Bound(initShards.toSet))

    var currentShards = initShards
    (1 to initShards.size).foreach { n =>
      var oldShards = currentShards
      val newShard = newAddress(initShards.size + n, Some(n))
      currentShards = currentShards.filter(!_.equals(initShards(n - 1))) ++ Seq(newShard)
      va() = Addr.Bound(currentShards.toSet)
      addrEquals(Addr.Bound(oldShards.toSet))
      pulse()
      addrEquals(Addr.Bound(currentShards.toSet))
    }
    pulse()
    addrEquals(Addr.Bound(currentShards.toSet))
  }

  test("multiple restarts in one epoch") { ctx =>
    import ctx._

    val initShards = Seq(shard1, shard2, shard3, shard4)
    va() = Addr.Bound(initShards.toSet)
    addrEquals(Addr.Bound(initShards.toSet))

    var currentShards = initShards
    (1 to initShards.size).foreach { n =>
      val newShard = newAddress(initShards.size + n, Some(n))
      currentShards = currentShards.filter(!_.equals(initShards(n - 1))) ++ Seq(newShard)
      va() = Addr.Bound(currentShards.toSet)
      addrEquals(Addr.Bound(initShards.toSet))
    }
    pulse()
    addrEquals(Addr.Bound(currentShards.toSet))
  }

  test("period=0; removals are immediate") { ctx =>
    val zeroCtx = ctx.copy(period = Duration.fromSeconds(0))
    import zeroCtx._

    // removals are immediate; addr2 leaves
    va() = Addr.Bound(addr1, addr2)
    addrEquals(Addr.Bound(addr1, addr2))
    va() = Addr.Bound(addr1)
    addrEquals(Addr.Bound(addr1))
    (0 to 100).foreach { _ =>
      pulse()
      addrEquals(Addr.Bound(addr1))
    }
  }

  test("period=0; additions are immediate") { ctx =>
    val zeroCtx = ctx.copy(period = Duration.fromSeconds(0))
    import zeroCtx._

    va() = Addr.Bound(addr1)
    addrEquals(Addr.Bound(addr1))

    va() = Addr.Bound(addr1, addr2)
    addrEquals(Addr.Bound(addr1, addr2))
  }

  test("period=0; bound is cached in presence of failures") { ctx =>
    val zeroCtx = ctx.copy(period = Duration.fromSeconds(0))
    import zeroCtx._

    va() = Addr.Bound(addr1, addr2)
    addrEquals(Addr.Bound(addr1, addr2))

    va() = Addr.Failed(new Exception)
    addrEquals(Addr.Bound(addr1, addr2))
  }
}
