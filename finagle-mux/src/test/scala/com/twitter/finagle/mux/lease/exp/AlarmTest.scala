package com.twitter.finagle.mux.lease.exp

import com.twitter.util.{Time, StorageUnit}
import com.twitter.conversions.time.intToTimeableNumber
import com.twitter.conversions.storage.intToStorageUnitableWholeNumber
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AlarmTest extends ExecElsewhere {

  test("DurationAlarm should work") {
    Time.withCurrentTimeFrozen { ctl =>
      val t = exec(
        { () =>
          Alarm.arm({ () =>
            new DurationAlarm(5.seconds)
          })
        },
        { () =>
          ctl.advance(5.seconds)
        }
      )
    }
  }

  test("MinAlarm should take the min time") {
    Time.withCurrentTimeFrozen { ctl =>
      exec(
        { () =>
          Alarm.arm({ () =>
            new DurationAlarm(5.seconds) min new DurationAlarm(2.seconds)
          })
        },
        { () =>
          ctl.advance(2.seconds)
        }
      )
    }
  }

  test("Alarm should continue if not yet finished") {
    Time.withCurrentTimeFrozen { ctl =>
      exec(
        { () =>
          Alarm.arm({ () =>
            new DurationAlarm(5.seconds) min new IntervalAlarm(1.second)
          })
        },
        { () =>
          ctl.advance(2.seconds)
        },
        { () =>
          ctl.advance(3.seconds)
        }
      )
    }
  }

  test("DurationAlarm should sleep until it's over") {
    Time.withCurrentTimeFrozen { ctl =>
      @volatile var ctr = 0
      exec(
        { () =>
          Alarm.armAndExecute({ () =>
            new DurationAlarm(5.seconds)
          }, { () =>
            ctr += 1
          })
        },
        { () =>
          assert(ctr === 1)
          ctl.advance(2.seconds)
        },
        { () =>
          assert(ctr === 1)
          ctl.advance(3.seconds)
        }
      )

      assert(ctr === 2)
    }
  }

  trait GenerationAlarmHelper {
    val fakePool = new FakeMemoryPool(new FakeMemoryUsage(StorageUnit.zero, 10.megabytes))
    val fakeBean = new FakeGarbageCollectorMXBean(0, 0)
    val nfo = new JvmInfo(fakePool, fakeBean)
    val ctr = FakeByteCounter(1, Time.now, nfo)
  }

  test("GenerationAlarm should sleep until the next alarm") {
    val h = new GenerationAlarmHelper{}
    import h._

    Time.withCurrentTimeFrozen { ctl =>
      exec(
        { () =>
          Alarm.arm({ () =>
            new GenerationAlarm(ctr) min new IntervalAlarm(1.second)
          })
        },
        { () =>
          fakeBean.getCollectionCount = 1
          ctl.advance(1.second)
        }
      )
    }
  }

  test("PredicateAlarm") {
    Time.withCurrentTimeFrozen { ctl =>
      @volatile var bool = false

      exec(
        { () =>
          Alarm.arm({ () =>
            new PredicateAlarm(() => bool) min new IntervalAlarm(1.second)
          })
        },
        { () =>
          bool = true
          ctl.advance(1.second)
        }
      )
    }
  }

  case class FakeByteCounter(rte: Long, gc: Time, nfo: JvmInfo) extends ByteCounter {
    def rate(): Long = rte
    def lastGc: Time = gc
    def info: JvmInfo = nfo
  }

  test("BytesAlarm should finish when we have enough bytes") {
    val h = new GenerationAlarmHelper{}
    import h._

    Time.withCurrentTimeFrozen { ctl =>
      val ctr = new FakeByteCounter(1000000, Time.now, nfo)
      @volatile var bool = false

      val usage = new FakeMemoryUsage(0.bytes, 10.megabytes)
      fakePool.setSnapshot(usage)

      exec(
        { () =>
          Alarm.arm({ () =>
            new BytesAlarm(ctr, () => 5.megabytes)
          })
        },
        { () =>
          fakePool.setSnapshot(usage.copy(used = 5.megabytes))
          ctl.advance(100.milliseconds)
        }
      )
    }
  }

  test("BytesAlarm should use 80% of the target") {
    val h = new GenerationAlarmHelper{}
    import h._
    val ctr = FakeByteCounter(50.megabytes.inBytes, Time.now, nfo)
    val alarm = new BytesAlarm(ctr, () => 5.megabytes)
    // 5MB / 50000000 B/S * 8 / 10 === 80.milliseconds
    // 80.milliseconds < 100.milliseconds
    assert(alarm.sleeptime === 80.milliseconds)
  }

  test("BytesAlarm should use the default if the gap is too big") {
    val h = new GenerationAlarmHelper{}
    import h._
    val ctr = FakeByteCounter(1000000, Time.now, nfo)
    val alarm = new BytesAlarm(ctr, () => 5.megabytes)
    // 5MB / 1000000B/S * 8 / 10 === 4.seconds
    // 4.seconds > 100.milliseconds
    assert(alarm.sleeptime === 100.milliseconds)
  }

  test("BytesAlarm should use zero if we're past") {
    val h = new GenerationAlarmHelper{}
    import h._
    val ctr = FakeByteCounter(1000000, Time.now, nfo)
    val alarm = new BytesAlarm(ctr, () => 5.megabytes)
    fakePool.setSnapshot(new FakeMemoryUsage(6.megabytes, 10.megabytes))
    // -1MB / 1000000B/S * 8 / 10 === -800.milliseconds
    // -800.milliseconds < 10.milliseconds
    assert(alarm.sleeptime === 10.milliseconds)
  }
}
