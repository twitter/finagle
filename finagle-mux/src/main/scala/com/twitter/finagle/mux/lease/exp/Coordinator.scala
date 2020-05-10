package com.twitter.finagle.mux.lease.exp

import com.twitter.conversions.StorageUnitOps._
import com.twitter.util.{Duration, Stopwatch}
import java.lang.management.{GarbageCollectorMXBean, MemoryPoolMXBean, ManagementFactory}
import java.util.logging.Logger
import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer

private[lease] class Coordinator(val counter: ByteCounter, verbose: Boolean = false) {

  /**
   * Wait until at least 80% of the committed space is
   * available
   */
  def gateCycle(): Unit = {
    Alarm.arm { () =>
      new PredicateAlarm(() => counter.info.remaining >= (counter.info.committed * 80 / 100)) min
        new BytesAlarm(counter, () => 0.bytes)
    }
  }

  // Warm up a bit: wait until we observe at least one
  // allocated byte.
  def warmup(): Unit = {
    Alarm.arm { () =>
      new BytesAlarm(
        counter, {
          val saved = counter.info.remaining()
          () => saved - 1.byte
        }
      )
    }
  }

  def sleepUntilGc(gc: () => Unit, interval: Duration): Unit = {
    Alarm.armAndExecute({ () => new GenerationAlarm(counter) min new IntervalAlarm(interval) }, gc)
  }

  // TODO: given that discount should be consistent for a generation, it doesn't
  // need to be rechecked.  This means that we could just check space.discount
  // once.  If that's the case, this will be broken without a GenerationAlarm
  // though.
  def sleepUntilDiscountRemaining(space: MemorySpace, fn: () => Unit): Unit = {
    // Since the discount might change while we're
    // sleeping; we re-check and loop until we know
    // that it has expired.
    //
    // TODO: wake up more often to see if the target
    // has changed.
    Alarm.armAndExecute({ () => new BytesAlarm(counter, () => space.discount()) }, fn)
  }

  def sleepUntilFinishedDraining(
    space: MemorySpace,
    maxWait: Duration,
    npending: () => Int,
    log: Logger
  ): Unit = {
    val elapsed = Stopwatch.start()
    // TODO: if grabbing memory info is slow, rewrite this to only check memory info occasionally
    Alarm.armAndExecute(
      { () =>
        new BytesAlarm(counter, () => space.left) min
          new DurationAlarm((maxWait - elapsed()) / 2) min
          new GenerationAlarm(counter) min
          new PredicateAlarm(() => npending() == 0)
      },
      { () =>
        // TODO MN: reenable
        if (verbose) {
          log.info(
            "DRAIN-LOOP: target=" +
              ((counter.info.remaining - space.minDiscount) / 100).inBytes + "; n=" + npending() +
              "; counter=" + counter + "; maxMs=" +
              ((maxWait - elapsed()) / 2).inMilliseconds.toInt
          )
        }
      }
    )
  }
}

private[lease] object Coordinator {

  /**
   * Try to make a Coordinator based on the JVM's underlying garbage collector.
   */
  def create(): Option[Coordinator] = {
    val ms = ManagementFactory.getMemoryPoolMXBeans().asScala
    val cs = ManagementFactory.getGarbageCollectorMXBeans().asScala
    parallelGc(ms, cs) orElse parNewCMS(ms, cs) map {
      case (memory, collector) =>
        val info = new JvmInfo(new BeanMemoryPool(memory), collector)
        val counter = new WindowedByteCounter(info)
        counter.start()
        new Coordinator(counter)
    }
  }

  /**
   * Try to get garbage stats for a ParScav+ParOld collected Java
   * process.
   */
  def parallelGc(
    ms: Buffer[MemoryPoolMXBean],
    cs: Buffer[GarbageCollectorMXBean]
  ): Option[(MemoryPoolMXBean, GarbageCollectorMXBean)] =
    for {
      parEden <- ms find (_.getName == "PS Eden Space")
      parScav <- cs find (_.getName == "PS Scavenge")
    } yield (parEden, parScav)

  /**
   * Try to to get garbage stats for a ParNew+CMS collected Java
   * process.
   */
  def parNewCMS(
    ms: Buffer[MemoryPoolMXBean],
    cs: Buffer[GarbageCollectorMXBean]
  ): Option[(MemoryPoolMXBean, GarbageCollectorMXBean)] =
    for {
      parEden <- ms find (_.getName == "Par Eden Space")
      parNew <- cs find (_.getName == "ParNew")
    } yield (parEden, parNew)
}
