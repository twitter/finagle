package com.twitter.finagle.mux.lease.exp

import com.twitter.conversions.storage.intToStorageUnitableWholeNumber
import com.twitter.conversions.time._
import com.twitter.util.{Time, Duration}
import java.lang.management.{GarbageCollectorMXBean, MemoryPoolMXBean}
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Logger
import org.junit.runner.RunWith
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import scala.collection.mutable.Buffer

@RunWith(classOf[JUnitRunner])
class CoordinatorTest extends FunSuite with LocalConductors with MockitoSugar {
  trait Ctx {
    val ctr = mock[ByteCounter]
    val coord = new Coordinator(ctr)
    val nfo = mock[JvmInfo]
    when(ctr.info).thenReturn(nfo)
    when(ctr.rate()).thenReturn(1)
  }

  test("Coordinator gateCycles properly") {
    val ctx = new Ctx{}
    import ctx._

    when(nfo.committed()).thenReturn(100.bytes)
    when(nfo.remaining())
      .thenReturn(60.bytes)

    val conductor = new Conductor
    import conductor._

    Time.withCurrentTimeFrozen { ctl =>
      localThread(conductor) {
        coord.gateCycle()
      }

      localThread(conductor) {
        waitForBeat(1)
        when(nfo.remaining())
          .thenReturn(80.bytes)
        ctl.advance(48.milliseconds) // 4/5 of 60 bytes * 1000 bps == 48.ms
      }

      conduct()
    }
  }

  test("Coordinator warms up properly") {
    val ctx = new Ctx{}
    import ctx._

    when(nfo.committed()).thenReturn(10.megabytes)
    when(nfo.remaining())
      .thenReturn(10.megabytes)

    val conductor = new Conductor
    import conductor._

    Time.withCurrentTimeFrozen { ctl =>

      localThread(conductor) {
        coord.warmup()
      }

      localThread(conductor) {
        waitForBeat(1)
        when(nfo.remaining())
          .thenReturn(10.megabytes - 2.byte)
        ctl.advance(10.millisecond)
      }

      conduct()
    }
  }

  test("Coordinator sleeps until gc") {
    val ctx = new Ctx{}
    import ctx._

    when(nfo.committed()).thenReturn(10.megabytes)
    when(nfo.remaining())
      .thenReturn(10.megabytes)
    when(nfo.generation()).thenReturn(0)

    @volatile var x = 0

    val conductor = new Conductor
    import conductor._

    Time.withCurrentTimeFrozen { ctl =>

      localThread(conductor) {
        coord.sleepUntilGc(
          { () => when(nfo.generation()).thenReturn(x) },
          20.milliseconds
        )
      }

      localThread(conductor) {
        waitForBeat(1)
        x = 1
        ctl.advance(20.millisecond)
      }

      conduct()
    }
  }

  test("test parallelGc scrapes the right beans") {
    val edenBean = mock[MemoryPoolMXBean]
    when(edenBean.getName).thenReturn("PS Eden Space")
    val scavBean = mock[GarbageCollectorMXBean]
    when(scavBean.getName).thenReturn("PS Scavenge")
    val oldBean = mock[GarbageCollectorMXBean]
    when(oldBean.getName).thenReturn("PS MarkSweep")
    val garbages = Buffer(scavBean, oldBean)
    val memories = Buffer(edenBean)
    assert(Coordinator.parallelGc(memories, garbages).isDefined)
  }

  test("test parNewCMS scrapes the right beans") {
    val edenBean = mock[MemoryPoolMXBean]
    when(edenBean.getName).thenReturn("Par Eden Space")
    val scavBean = mock[GarbageCollectorMXBean]
    when(scavBean.getName).thenReturn("ParNew")
    val oldBean = mock[GarbageCollectorMXBean]
    when(oldBean.getName).thenReturn("ConcurrentMarkSweep")
    val garbages = Buffer(scavBean, oldBean)
    val memories = Buffer(edenBean)
    assert(Coordinator.parNewCMS(memories, garbages).isDefined)
  }

  if (!sys.props.contains("SKIP_FLAKY"))
  test("Coordinator sleeps until discount remaining") {
    val ctx = new Ctx{}
    import ctx._

    val space = mock[MemorySpace]
    when(space.discount()).thenReturn(5.megabytes)
    when(nfo.remaining())
      .thenReturn(10.megabytes)

    @volatile var incr = 0

    val conductor = new Conductor
    import conductor._

    Time.withCurrentTimeFrozen { ctl =>
      localThread(conductor) {
        coord.sleepUntilDiscountRemaining(space, () => incr += 1)
      }

      localThread(conductor) {
        waitForBeat(1)
        when(nfo.remaining())
          .thenReturn(5.megabytes)
        ctl.advance(100.milliseconds)
      }
    }

    localWhenFinished(conductor) {
      assert(incr == 2)
    }
  }

  test("Coordinator sleeps until finished draining") {
    val ctx = new Ctx{}
    import ctx._

    val space = mock[MemorySpace]
    when(space.left).thenReturn(5.megabytes)

    when(nfo.committed()).thenReturn(5.megabytes)
    when(nfo.remaining())
      .thenReturn(10.megabytes)
    when(nfo.generation()).thenReturn(0)

    val maxWait = Duration.Top
    @volatile var npending = 1
    val log = Logger.getAnonymousLogger()

    val conductor = new Conductor
    import conductor._

    Time.withCurrentTimeFrozen { ctl =>
      localThread(conductor) {
        coord.sleepUntilFinishedDraining(
          space,
          maxWait,
          () => npending,
          log
        )
      }

      localThread(conductor) {
        npending = 0
        ctl.advance(100.milliseconds)
      }

      conduct()
    }
  }
}
