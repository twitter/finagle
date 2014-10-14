package com.twitter.finagle.mux.lease.exp

import com.twitter.conversions.storage.intToStorageUnitableWholeNumber
import com.twitter.conversions.time._
import com.twitter.util.{Time, Duration}
import java.lang.management.{GarbageCollectorMXBean, MemoryPoolMXBean}
import java.util.logging.Logger
import org.junit.runner.RunWith
import org.mockito.Mockito.{when, verify}
import org.scalatest.FunSuite
import org.scalatest.concurrent.Eventually
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import scala.collection.mutable.Buffer

@RunWith(classOf[JUnitRunner])
class CoordinatorTest extends ExecElsewhere with MockitoSugar {
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

    Time.withCurrentTimeFrozen { ctl =>
      exec(
        { () =>
          coord.gateCycle()
        },
        { () =>
          when(nfo.remaining())
            .thenReturn(80.bytes)
          ctl.advance(48.milliseconds) // 4/5 of 60 bytes * 1000 bps === 48.ms
        }
      )
    }
  }

  test("Coordinator warms up properly") {
    val ctx = new Ctx{}
    import ctx._

    when(nfo.committed()).thenReturn(10.megabytes)
    when(nfo.remaining())
      .thenReturn(10.megabytes)

    Time.withCurrentTimeFrozen { ctl =>
      exec(
        { () =>
          coord.warmup()
        },
        { () =>
          when(nfo.remaining())
            .thenReturn(10.megabytes - 2.byte)
          ctl.advance(10.millisecond)
        }
      )
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

    Time.withCurrentTimeFrozen { ctl =>
      exec(
        { () =>
          coord.sleepUntilGc(
            { () => when(nfo.generation()).thenReturn(x) },
            20.milliseconds
          )
        },
        { () =>
          x = 1
          ctl.advance(20.millisecond)
        }
      )
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

  test("Coordinator sleeps until discount remaining") {
    val ctx = new Ctx{}
    import ctx._

    val space = mock[MemorySpace]
    when(space.discount()).thenReturn(5.megabytes)
    when(nfo.remaining())
      .thenReturn(10.megabytes)

    @volatile var incr = 0

    Time.withCurrentTimeFrozen { ctl =>
      exec(
        { () =>
          coord.sleepUntilDiscountRemaining(space, () => incr += 1)
        },
        { () =>
          when(nfo.remaining())
            .thenReturn(5.megabytes)
          ctl.advance(100.milliseconds)
        }
      )
    }

    assert(incr === 2)
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

    Time.withCurrentTimeFrozen { ctl =>
      exec(
        { () =>
          coord.sleepUntilFinishedDraining(
            space,
            maxWait,
            () => npending,
            log
          )
        },
        { () =>
          npending = 0
          ctl.advance(100.milliseconds)
        }
      )
    }
  }
}
