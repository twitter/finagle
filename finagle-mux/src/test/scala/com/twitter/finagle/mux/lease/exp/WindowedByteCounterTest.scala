package com.twitter.finagle.mux.lease.exp

import com.twitter.util._
import com.twitter.util.Local.Context
import com.twitter.conversions.storage.intToStorageUnitableWholeNumber
import com.twitter.conversions.time.intToTimeableNumber
import org.junit.runner.RunWith
import org.scalatest.concurrent.Eventually
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class WindowedByteCounterTest extends FunSuite with Eventually {
  trait ByteCounterHelper {
    val fakePool = new FakeMemoryPool(new FakeMemoryUsage(StorageUnit.zero, StorageUnit.zero))
    val fakeBean = new FakeGarbageCollectorMXBean(0, 0)
    val nfo = new JvmInfo(fakePool, fakeBean)
  }

  // cleans up thread
  def withCounter(fakeBean: FakeGarbageCollectorMXBean, fakePool: FakeMemoryPool)(
    fn: (ByteCounter, () => Unit) => Unit
  ): Unit = {
    Time.withCurrentTimeFrozen { ctl =>
      val nfo = new JvmInfo(fakePool, fakeBean)
      val counter = new WindowedByteCounter(nfo, Local.save())
      eventually {
        assert(counter.getState === Thread.State.TIMED_WAITING)
      }

      @volatile var closed = false
      @volatile var prev = -1
      val nextPeriod = { () =>
        eventually {
          assert(counter.getState === Thread.State.TIMED_WAITING)
        }
        ctl.advance(WindowedByteCounter.P)
        eventually {
          assert(counter.passCount != prev)
        }

        prev = counter.passCount

        if (!closed) eventually {
          assert(counter.getState === Thread.State.TIMED_WAITING)
        }
      }

      fn(counter, nextPeriod)

      counter.close()
      closed = true
      nextPeriod()

      counter.join()
    }
  }

  test("ByteCounter should be stoppable") {
    val h = new ByteCounterHelper{}
    import h._

    Time.withCurrentTimeFrozen { ctl =>
      val counter = new WindowedByteCounter(nfo, Local.save())

      assert(counter.close().poll === Some(Return(())))

      counter.join()
      assert(counter.isAlive === false)
    }
  }

  test("ByteCounter should give a trivial rate without info") {
    val h = new ByteCounterHelper{}
    import h._

    withCounter(fakeBean, fakePool) { case (counter, _) =>
      assert(counter.rate() === 0)
    }
  }

  if (!sys.props.contains("SKIP_FLAKY")) test("ByteCounter should accurately measure rate") {
    val h = new ByteCounterHelper{}
    import h._

    withCounter(fakeBean, fakePool) { case (counter, nextPeriod) =>
      val usage = new FakeMemoryUsage(0.bytes, 10.megabytes)
      for (i <- 0 until WindowedByteCounter.N) {
        fakePool.setSnapshot(usage.copy(used = (1 + i).kilobytes))
        nextPeriod()
      }

      assert(counter.rate() === (WindowedByteCounter.N.kilobytes).inBytes / WindowedByteCounter.W.inSeconds)
    }
  }

  if (!sys.props.contains("SKIP_FLAKY")) test("ByteCounter should support a windowed rate") {
    val h = new ByteCounterHelper{}
    import h._

    withCounter(fakeBean, fakePool) { case (counter, nextPeriod) =>
      val usage = new FakeMemoryUsage(0.bytes, 10.megabytes)
      for (i <- 1 to WindowedByteCounter.N) {
        fakePool.setSnapshot(usage.copy(used = i.kilobytes))
        nextPeriod()
      }

      assert(counter.rate() === (WindowedByteCounter.N.kilobytes).inBytes / WindowedByteCounter.W.inSeconds)

      for (i <- 1 to WindowedByteCounter.N) {
        fakePool.setSnapshot(usage.copy(used = WindowedByteCounter.N.kilobytes + (i * 2).kilobytes))
        nextPeriod()
      }

      assert(counter.rate() === (2 * (WindowedByteCounter.N.kilobytes).inBytes / WindowedByteCounter.W.inSeconds))
    }
  }

  test("ByteCounter should calculate a rate even for weird values") {
    val h = new ByteCounterHelper{}
    import h._

    withCounter(fakeBean, fakePool) { case (counter, nextPeriod) =>
      val usage = new FakeMemoryUsage(0.bytes, 10.megabytes)
      var x = StorageUnit.zero
      val rand = new Random(0)

      for (i <- 0 until WindowedByteCounter.N) {
        x += rand.nextInt(100).kilobytes
        fakePool.setSnapshot(usage.copy(used = x))
        nextPeriod()
      }

      assert(counter.rate() === x.inBytes / WindowedByteCounter.W.inSeconds)
    }
  }

  if (!sys.props.contains("SKIP_FLAKY")) test("Doing a gc should make us roll over, and should not count the gc") {
    val h = new ByteCounterHelper{}
    import h._

    withCounter(fakeBean, fakePool) { case (counter, nextPeriod) =>
      val usage = new FakeMemoryUsage(0.bytes, 10.megabytes)
      var x = StorageUnit.zero

      for (i <- 0 until WindowedByteCounter.N / 2) {
        x += 1.kilobytes
        fakePool.setSnapshot(usage.copy(used = x))
        nextPeriod()
      }

      x = StorageUnit.zero
      // bump gc number
      fakeBean.getCollectionCount = 1
      fakePool.setSnapshot(usage)
      nextPeriod()

      for (i <- WindowedByteCounter.N / 2 until WindowedByteCounter.N) {
        x += 1.kilobytes
        fakePool.setSnapshot(usage.copy(used = x))
        nextPeriod()
      }

      assert(counter.rate() === WindowedByteCounter.N.kilobytes.inBytes / WindowedByteCounter.W.inSeconds)
    }
  }

  if (!sys.props.contains("SKIP_FLAKY")) test("Keep track of last gc time") {
    val h = new ByteCounterHelper{}
    import h._

    withCounter(fakeBean, fakePool) { case (counter, nextPeriod) =>
      val usage = new FakeMemoryUsage(0.bytes, 10.megabytes)
      var x = StorageUnit.zero

      assert(counter.lastGc === Time.now)

      for (i <- 0 until WindowedByteCounter.N / 2) {
        x += 1.kilobytes
        fakePool.setSnapshot(usage.copy(used = x))
        nextPeriod()
      }

      x = StorageUnit.zero
      // bump gc number
      fakeBean.getCollectionCount = 1
      fakePool.setSnapshot(usage)
      nextPeriod()
      val saved = Time.now

      for (i <- WindowedByteCounter.N / 2 until WindowedByteCounter.N) {
        x += 1.kilobytes
        fakePool.setSnapshot(usage.copy(used = x))
        nextPeriod()
      }

      assert(counter.lastGc === saved)
    }
  }
}
