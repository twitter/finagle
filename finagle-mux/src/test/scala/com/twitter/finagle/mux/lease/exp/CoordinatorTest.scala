package com.twitter.finagle.mux.lease.exp

import com.twitter.util.Time
import com.twitter.conversions.storage.intToStorageUnitableWholeNumber
import com.twitter.conversions.time.intToTimeableNumber
import org.junit.runner.RunWith
import org.scalatest.concurrent.Eventually
import org.scalatest.FunSuite
import org.mockito.Mockito.{when, verify}
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class CoordinatorTest extends ExecElsewhere with MockitoSugar {
  trait Ctx {
    val ctr = mock[ByteCounter]
    val coord = new Coordinator(ctr)
    val nfo = mock[JvmInfo]
    when(ctr.info).thenReturn(nfo)
    when(ctr.rate()).thenReturn(1000)
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
}
