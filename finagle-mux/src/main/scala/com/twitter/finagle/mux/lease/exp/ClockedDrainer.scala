package com.twitter.finagle.mux.lease.exp

import com.twitter.app.GlobalFlag
import com.twitter.conversions.storage.intToStorageUnitableWholeNumber
import com.twitter.conversions.time._
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver, DefaultStatsReceiver}
import com.twitter.util.{Duration, Stopwatch, StorageUnit, Timer, Time, NilStopwatch}
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Logger

/**
 * ClockedDrainer is a thread which keeps track of the garbage collector and
 * guesses when it will pause.
 *
 * @param coord controls how long we sleep for
 * @param forceGc forces a gc on invocation
 * @param space represents the current state of the JVM's heap
 * @param rSnooper keeps track of request rate vis a vis heap allocations
 * @param log is used for logging, will be removed later
 * @param lr is used for logging things in a pretty way
 * @param statsReceiver keeps track of the stats
 * @param verbose whether the logging should be verbose or not
 */
// NB: this is a mess, but it's actually fairly straightforward.
// There are four stages, wrapped in an infinite loop.
// The stages are:
// Wait until close enough
// Drain
// GC
// Undrain
private[finagle] class ClockedDrainer(
  coord: Coordinator,
  forceGc: () => Unit,
  space: MemorySpace,
  rSnooper: RequestSnooper,
  log: Logger,
  lr: LogsReceiver = NullLogsReceiver,
  statsReceiver: StatsReceiver = NullStatsReceiver,
  verbose: Boolean = false
) extends Thread("GcDrainer") with Lessor {

  private[this] val lessees = Collections.newSetFromMap(
    new ConcurrentHashMap[Lessee, java.lang.Boolean])

  private[this] val requestCount = new AtomicInteger(0)
  private[this] val narrival = new AtomicInteger(0)

  @volatile private[this] var openFor = Stopwatch.start()
  @volatile private[this] var closedFor = NilStopwatch.start()
  @volatile private[this] var forcedGc = 0L
  @volatile private[this] var genDrained, genOpen = 0L

  private[this] def calculateMaxWait: Duration = {
    val rate = coord.counter.rate
    val r = space.left
    if (r <= StorageUnit.zero) Duration.Zero
    else if (rate <= 0) 10.milliseconds
    else (r.inBytes / rate).toLong.milliseconds
  }

  private object stats {
    val undrain = statsReceiver.counter("undrain")
    val drain = statsReceiver.counter("drain")
    val forcedGcs = statsReceiver.counter("forcedgcs")
    val naturalGcs = statsReceiver.counter("naturalgcs")
    val pendingAtGc = statsReceiver.stat("pendingatgc")
    val pendingAtDrain = statsReceiver.stat("pendingatdrain")

    val drainTime = statsReceiver.stat("draintime_ms")

    val openTime = statsReceiver.stat("opentime_ms")
    val closedTime = statsReceiver.stat("closedtime_ms")

    val openForGauge = statsReceiver.addGauge("openfor_ms") {
      openFor() match {
        case Duration.Finite(d) => d.inMilliseconds.toFloat
        case _ => -1F
      }
    }

    val closedForGauge = statsReceiver.addGauge("closedfor_ms") {
      closedFor() match {
        case Duration.Finite(d) => d.inMilliseconds.toFloat
        case _ => -1F
      }
    }

    val discountGauge = statsReceiver.addGauge("discount") { space.discount().inBytes.toFloat }
  }

  def npending() = {
    var s = 0
    val iter = lessees.iterator()
    while (iter.hasNext)
      s += iter.next().npending()
    s
  }

  private[this] def upkeep(state: String, init: () => Duration) {
    lr.record("%s_ms".format(state), init().inMilliseconds.toString)
    lr.record("count_%s".format(state), requestCount.get().toString)
    lr.record("pending_%s".format(state), npending().toString)
    lr.record("arrival_%s".format(state), narrival.get().toString)
    coord.counter.info.record(lr, state)
  }

  override def run() {
    var ncycles = 0L

    val init = Stopwatch.start()

    coord.warmup()
    while (true) {
      ready(init)

      val g: Long = coord.counter.info.generation()

      upkeep("closed", init)
      drain()
      upkeep("drained", init)

      gc(g, init)

      undrain()

      ncycles += 1
      lr.record("cycle", ncycles.toString)
      flushLogs()
    }
  }

  setDaemon(true)
  start()

  // READY
  private[lease] def ready(init: () => Duration) { // private[lease] for testing
    lr.record("gate_open_ms", init().inMilliseconds.toString)
    coord.gateCycle()

    upkeep("open", init)

    coord.sleepUntilDiscountRemaining(space, { () =>
      if (verbose) {
        log.info("AWAIT-DISCOUNT: discount="+space.discount()+
          "; clock="+coord.counter +
          "; space="+space

        )
      }

      // discount (bytes) / rate (bytes / second) == expiry (seconds)
      issueAll((space.discount.inBytes / coord.counter.rate).toLong.seconds)
    })
  }

  // DRAINING
  private[lease] def drain() { // private[lease] for testing
    val sinceClosed = Stopwatch.start()
    startDraining()
    finishDraining()
    stats.drainTime.add(sinceClosed().inMilliseconds)
  }

  private[this] def startDraining() {
    stats.openTime.add(openFor().inMilliseconds)
    openFor = NilStopwatch.start()
    closedFor = Stopwatch.start()

    stats.drain.incr()
    stats.pendingAtDrain.add(npending())
    issueAll(Duration.Zero)
  }

  private[this] def issueAll(duration: Duration) {
    val iter = lessees.iterator()
    while (iter.hasNext)
      iter.next().issue(duration)
  }

  private[this] def finishDraining() {
    val maxWait = calculateMaxWait

    if (verbose) {
      log.info("AWAIT-DRAIN: n="+npending()+
        "; clock="+coord.counter+
        "; space="+space+
        "; maxWaitMs="+maxWait.inMilliseconds+
        "; minDiscount="+space.minDiscount)
    }

    coord.sleepUntilFinishedDraining(space, maxWait, npending, log)
  }

  // GC
  // loop until the gc is acknowledged
  private[lease] def gc(generation: Long, init: () => Duration) { // private[lease] for testing
    val elapsedGc = Stopwatch.start()

    forcedGc = 0
    if (coord.counter.info.generation() == generation) {
      val n = npending()
      if (verbose) log.info("FORCE-GC: n="+n+"; clock="+coord.counter+"; space="+space)

      lr.record("byteLeft", coord.counter.info.remaining().inBytes.toString)

      forcedGc = 0
      coord.sleepUntilGc({ () =>
        forceGc()
        forcedGc += 1
      }, 10.milliseconds)

      stats.pendingAtGc.add(n)
      stats.forcedGcs.incr()
    } else {
      if (verbose) log.info("NATURAL-GC")
      lr.record("byteLeft", -1.toString)
      stats.naturalGcs.incr()
    }

    upkeep("done", init)

    val gcMs = elapsedGc().inMilliseconds
    lr.record("gcMs", gcMs.toString)
  }

  // UNDRAINING
  private[lease] def undrain() { // private[lease] for testing
    stats.closedTime.add(closedFor().inMilliseconds)
    openFor = Stopwatch.start()
    closedFor = NilStopwatch.start()

    stats.undrain.incr()
    issueAll(Duration.Top)
  }

  // FLUSHING
  private[this] def flushLogs() {
    lr.record("gendiff", (genDrained - genOpen).toString)
    lr.record("forcedGc", forcedGc.toString)

    lr.flush()
  }

  // TODO: can this API be made easier to use?
  def register(lessee: Lessee) {
    lessees.add(lessee)
    // TODO: issue leases immediately.
    // currently there's a bit of startup cost if
    // a client joins while we're draining.
  }

  def unregister(lessee: Lessee) {
    lessees.remove(lessee)
  }

  def observe(d: Duration) {
    requestCount.incrementAndGet()
    rSnooper.observe(d)
  }

  def observeArrival() {
    narrival.incrementAndGet()
  }
}


object drainerDiscountRange extends GlobalFlag(
  (50.megabytes, 600.megabytes), "Range of discount")

object drainerPercentile extends GlobalFlag(95, "GC drainer cutoff percentile")

object drainerDebug extends GlobalFlag(false, "GC drainer debug log (verbose)")
object drainerEnabled extends GlobalFlag(false, "GC drainer enabled")

object nackOnExpiredLease extends GlobalFlag(false, "nack when the lease has expired")

private[finagle] object ClockedDrainer {
  private[this] val log = Logger.getLogger("ClockedDrainer")
  private[this] val lr = if (drainerDebug()) new DedupingLogsReceiver(log) else NullLogsReceiver

  lazy val flagged: Lessor = if (drainerEnabled()) {
    Coordinator.create() match {
      case None =>
        log.warning("Failed to acquire a ParNew+CMS Coordinator; cannot "+
          "construct drainer")
        Lessor.nil
      case Some(coord) =>
        val rSnooper = new RequestSnooper(
          coord.counter,
          drainerPercentile().toDouble / 100.0,
          lr
        )

        val (min, max) = drainerDiscountRange()
        assert(min < max)

        val space = new MemorySpace(
          coord.counter.info,
          min,
          max,
          rSnooper,
          lr
        )

        new ClockedDrainer(
          coord,
          GarbageCollector.forceNewGc,
          space,
          rSnooper,
          log,
          lr,
          DefaultStatsReceiver.scope("gcdrainer")
        )
    }
  } else {
    log.info("Drainer is disabled; bypassing")
    Lessor.nil
  }
}
