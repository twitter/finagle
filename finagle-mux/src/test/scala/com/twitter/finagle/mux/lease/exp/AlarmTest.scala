package com.twitter.finagle.mux.lease.exp

import com.twitter.util.{StorageUnit, Time}
import com.twitter.conversions.DurationOps._
import com.twitter.conversions.StorageUnitOps._
import org.scalactic.source.Position
import org.scalatest.Tag
import org.scalatest.funsuite.AnyFunSuite

class AlarmTest extends AnyFunSuite with LocalConductors {

  val skipWholeTest: Boolean = sys.props.contains("SKIP_FLAKY")

  override def test(
    testName: String,
    testTags: Tag*
  )(
    testFun: => Any
  )(
    implicit pos: Position
  ): Unit = {
    if (skipWholeTest)
      ignore(testName)(testFun)
    else
      super.test(testName, testTags: _*)(testFun)
  }

  test("DurationAlarm should work") {
    val conductor = new Conductor
    import conductor._

    Time.withCurrentTimeFrozen { ctl =>
      localThread(conductor) {
        Alarm.arm({ () => new DurationAlarm(5.seconds) })
      }

      localThread(conductor) {
        waitForBeat(1)
        ctl.advance(5.seconds)
      }

      conduct()
    }
  }

  test("MinAlarm should take the min time") {
    val conductor = new Conductor
    import conductor._

    Time.withCurrentTimeFrozen { ctl =>
      localThread(conductor) {
        Alarm.arm({ () => new DurationAlarm(5.seconds) min new DurationAlarm(2.seconds) })
      }

      localThread(conductor) {
        waitForBeat(1)
        ctl.advance(2.seconds)
      }

      conduct()
    }
  }

  test("Alarm should continue if not yet finished") {
    val conductor = new Conductor
    import conductor._

    Time.withCurrentTimeFrozen { ctl =>
      localThread(conductor) {
        Alarm.arm({ () => new DurationAlarm(5.seconds) min new IntervalAlarm(1.second) })
      }

      localThread(conductor) {
        waitForBeat(1)
        ctl.advance(2.seconds)
        waitForBeat(2)
        ctl.advance(3.seconds)
      }

      conduct()
    }
  }

  if (!sys.props.contains("SKIP_FLAKY"))
    test("DurationAlarm should sleep until it's over") {
      val conductor = new Conductor
      import conductor._

      @volatile var ctr = 0

      Time.withCurrentTimeFrozen { ctl =>
        localThread(conductor) {
          Alarm.armAndExecute({ () => new DurationAlarm(5.seconds) }, { () => ctr += 1 })
        }

        localThread(conductor) {
          waitForBeat(1)
          assert(ctr == 1)
          ctl.advance(2.seconds)

          waitForBeat(2)
          assert(ctr == 1)
          ctl.advance(3.seconds)
        }
      }

      localWhenFinished(conductor) {
        assert(ctr == 2)
      }
    }

  trait GenerationAlarmHelper {
    val fakePool = new FakeMemoryPool(new FakeMemoryUsage(StorageUnit.zero, 10.megabytes))
    val fakeBean = new FakeGarbageCollectorMXBean(0, 0)
    val nfo = new JvmInfo(fakePool, fakeBean)
    val ctr = FakeByteCounter(1, Time.now, nfo)
  }

  test("GenerationAlarm should sleep until the next alarm") {
    val h = new GenerationAlarmHelper {}
    import h._

    val conductor = new Conductor
    import conductor._

    Time.withCurrentTimeFrozen { ctl =>
      localThread(conductor) {
        Alarm.arm({ () => new GenerationAlarm(ctr) min new IntervalAlarm(1.second) })
      }

      localThread(conductor) {
        waitForBeat(1)
        fakeBean.getCollectionCount = 1
        ctl.advance(1.second)
      }

      conduct()
    }
  }

  test("PredicateAlarm") {
    val conductor = new Conductor
    import conductor._

    Time.withCurrentTimeFrozen { ctl =>
      @volatile var bool = false

      localThread(conductor) {
        Alarm.arm({ () => new PredicateAlarm(() => bool) min new IntervalAlarm(1.second) })
      }

      localThread(conductor) {
        waitForBeat(1)
        bool = true
        ctl.advance(1.second)
      }

      conduct()
    }
  }

  case class FakeByteCounter(rte: Double, gc: Time, nfo: JvmInfo) extends ByteCounter {
    def rate(): Double = rte
    def lastGc: Time = gc
    def info: JvmInfo = nfo
  }

  test("BytesAlarm should finish when we have enough bytes") {
    val h = new GenerationAlarmHelper {}
    import h._

    val conductor = new Conductor
    import conductor._

    Time.withCurrentTimeFrozen { ctl =>
      val ctr = new FakeByteCounter(1000, Time.now, nfo)
      @volatile var bool = false

      val usage = new FakeMemoryUsage(0.bytes, 10.megabytes)
      fakePool.setSnapshot(usage)

      localThread(conductor) {
        Alarm.arm({ () => new BytesAlarm(ctr, () => 5.megabytes) })
      }

      localThread(conductor) {
        waitForBeat(1)
        fakePool.setSnapshot(usage.copy(used = 5.megabytes))
        ctl.advance(100.milliseconds)
      }

      conduct()
    }
  }

  test("BytesAlarm should use 80% of the target") {
    val h = new GenerationAlarmHelper {}
    import h._
    val ctr = FakeByteCounter(50.kilobytes.inBytes, Time.now, nfo)
    val alarm = new BytesAlarm(ctr, () => 5.megabytes)
    // 5MB / (50 KB/ms) * 8 / 10 == 80.milliseconds
    // 80.milliseconds < 100.milliseconds
    assert(alarm.sleeptime == ((80 * 1.kilobyte.inBytes / 1000).milliseconds))
  }

  test("BytesAlarm should use the default if the gap is too big") {
    val h = new GenerationAlarmHelper {}
    import h._
    val ctr = FakeByteCounter(1000, Time.now, nfo)
    val alarm = new BytesAlarm(ctr, () => 5.megabytes)
    // 5MB / 1000000B/S * 8 / 10 == 4.seconds
    // 4.seconds > 100.milliseconds
    assert(alarm.sleeptime == 100.milliseconds)
  }

  test("BytesAlarm should use zero if we're past") {
    val h = new GenerationAlarmHelper {}
    import h._
    val ctr = FakeByteCounter(1000, Time.now, nfo)
    val alarm = new BytesAlarm(ctr, () => 5.megabytes)
    fakePool.setSnapshot(new FakeMemoryUsage(6.megabytes, 10.megabytes))
    // -1MB / 1000000B/S * 8 / 10 == -800.milliseconds
    // -800.milliseconds < 10.milliseconds
    assert(alarm.sleeptime == 10.milliseconds)
  }
}
