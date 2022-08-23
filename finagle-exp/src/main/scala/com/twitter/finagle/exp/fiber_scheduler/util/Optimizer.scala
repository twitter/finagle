package com.twitter.finagle.exp.fiber_scheduler.util

import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.logging.Logger
import com.twitter.util.Await
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.TimeoutException
import com.twitter.finagle.exp.fiber_scheduler.statsReceiver
import java.util.Arrays
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Optimizer implementation that constantly probes if adding or removing
 * resources increases the score. Uses the following steps:
 *
 * 1. Every `cycleInterval`, collects the score and adds it to a circular
 * array buffer
 * 2. Every `cycleInterval` * `wavePeriod`, changes the phase of the optimizer
 * by adding or removing one resource
 * 3. Every `cycleInterval` * `adaptPeriod`, makes a adaptation decision based on
 * the collected scores and adds or removes a resource
 *
 * In addition to this feedback loop, the optimizer respects the `min` and `max`
 * parameters and checks the cliff and valley limits every `cycleInterval`. Cliffs
 * have priority over Valleys in case both are detected.
 */
private[fiber_scheduler] final case class Optimizer(
  score: Optimizer.Score,
  cliffLimit: Optimizer.Limit,
  valleyLimit: Optimizer.Limit,
  cliffExpiration: Duration,
  valleyExpiration: Duration,
  max: Int,
  min: Int,
  get: () => Int,
  up: () => Future[Unit],
  down: () => Future[Unit],
  isIdle: () => Boolean,
  adaptPeriod: Int,
  wavePeriod: Int,
  cycleInterval: Duration) {

  import Optimizer._

  assert(
    !cycleInterval.isZero && cycleInterval.isFinite,
    "cycleInterval interval must be greater than zero and finite")
  assert(adaptPeriod % wavePeriod == 0, "adaptPeriod must be a multiple of wavePeriod")
  assert((adaptPeriod & (adaptPeriod - 1)) == 0, "adaptPeriod must be a power of two")
  assert((wavePeriod & (wavePeriod - 1)) == 0, "wavePeriod must be a power of two")
  assert(max - min > 3, "max - min > 3")

  private[this] val stats = new Stats
  private[this] val scores = new Array[Int](adaptPeriod)
  private[this] val values = new Array[Int](adaptPeriod)

  private[this] var index = 0
  private[this] var phase: Phase = Up

  private[this] val cliff =
    new ExpiringValue[Int](max, cliffExpiration, v => log.info(s"cliff at $v expired"))
  private[this] val valley =
    new ExpiringValue[Int](min, valleyExpiration, v => log.info(s"valley at $v expired"))

  // use a dedicated thread to reliably execute the feedback loop
  private[this] val exec = Executors.newScheduledThreadPool(
    1,
    new NamedPoolThreadFactory("fiber/optimizer", makeDaemons = true))
  exec.scheduleAtFixedRate(() => run(), 0, cycleInterval.inNanoseconds, TimeUnit.NANOSECONDS)

  /**
   * Main optimization loop executed every `cycleInterval`
   */
  private[this] def run() = {
    val start = System.nanoTime()
    try {
      update()
      if (!isIdle()) {
        checkLimits()
      }
      if (index == 0) {
        adapt()
      }
      if ((index & (wavePeriod - 1)) == 0) {
        changePhase()
      }
    } catch {
      case ex: Throwable =>
        log.error(ex, "optimizer failure")
        stats.failure.incr()
    } finally {
      stats.latency.add((System.nanoTime() - start) / 1000000)
    }
  }

  def stop(): Unit = {
    exec.shutdown()
    stats.remove()
  }

  /**
   * Collects the score and current value
   */
  private[this] def update() = {
    val curr = get()
    val s = score.get()

    scores(index) =
      if (phase eq Up) s
      else -s
    values(index) = curr
    index = (index + 1) & (adaptPeriod - 1)

    stats.update.incr()
  }

  /**
   * Checks if limits have been reached and reacts accordingly
   * by adding or removed one resource. Cliffs have priority
   * over valleys. If both are detected, the valley is adjusted
   * to the current cliff - 2.
   */
  private[this] def checkLimits() = {
    val curr = get()
    if (cliffLimit.get()) {
      cliff.set(curr.max(min + 2))
    }
    if (valleyLimit.get()) {
      valley.set(curr)
    }
    if (cliff() - valley() <= 2) {
      valley.set(cliff() - 2, refreshExpiration = false)
    }
    if (cliff() == curr) {
      log.info(s"reached cliff at $curr, moving down")
      moveDown()
      cliffLimit.cleanup()
    } else if (valley() == curr) {
      log.info(s"reached valley at $curr, moving up")
      moveUp()
      valleyLimit.cleanup()
    }
  }

  // Makes an adaptation decision
  private[this] def adapt() = {
    val curr = get()
    val score = scores.sum
    val noise = this.noise()

    stats.adaptScore.add(score)
    stats.adaptNoise.add(noise)

    log.info(
      s"adapting [absScore=${scores.map(Math.abs).sum}, score=$score, noise=$noise, curr=$curr]")
    log.info(s"scores: ${Arrays.toString(scores)}")
    log.info(s"values: ${Arrays.toString(values)}")

    // Even if the latest cliff and valley are expired, take
    // them into consideration as soft limits

    def cliffDist() = {
      val c = cliff.getExpired()
      if (curr < c) {
        c - curr
      } else {
        cliff() - curr
      }
    }

    def valleyDist() = {
      val v = valley.getExpired()
      if (curr > v) {
        curr - v
      } else {
        curr - valley()
      }
    }

    // The noise is attenuated based on the distance of
    // the cliff or valley. The closest to a limit, the clearer
    // the signal must be.
    if (isIdle()) {
      stats.idle.incr()
      log.info("idle, no change")
    } else if (score > noise / Math.sqrt(cliffDist())) {
      log.info("adapt up")
      if (phase == Up) {
        phase = Down
      } else {
        moveUp()
      }
    } else if (score < -noise / Math.sqrt(valleyDist())) {
      log.info("adapt down")
      if (phase == Down) {
        phase = Up
      } else {
        moveDown()
      }
    } else {
      stats.noChange.incr()
      log.info("no change")
    }
    stats.adapt.incr()
  }

  private[this] def changePhase() = {
    if (phase eq Up) {
      moveDown()
      phase = Down
    } else {
      moveUp()
      phase = Up
    }
    stats.changePhase.incr()
  }

  private[this] def moveUp() =
    if (get() < cliff() - 1) {
      log.info(s"up to ${get() + 1} [cliff = $cliff, valley = $valley]")
      await(up())
      stats.up.incr()
    } else {
      log.info(s"up to ${get() + 1} ignored [cliff = $cliff, valley = $valley]")
    }

  private[this] def moveDown() =
    if (get() > valley() + 1) {
      log.info(s"down to ${get() - 1} [cliff = $cliff, valley = $valley]")
      await(down())
      stats.down.incr()
    } else {
      log.info(s"down to ${get() - 1} ignored [cliff = $cliff, valley = $valley]")
    }

  private[this] def await(f: Future[Unit]) =
    try Await.result(f, cycleInterval / 2)
    catch {
      case ex: TimeoutException =>
        log.warning(s"move timeout after ${cycleInterval / 2}", ex)
    }

  /**
   * Measures noise as the max range of the up and down scores.
   */
  private[this] def noise() = {
    def range(l: List[Int]) =
      l match {
        case Nil => 0
        case l => l.max - l.min
      }
    val (downScores, upScores) =
      scores.toList.partition(_ < 0)

    range(upScores).max(range(downScores))
  }

  private[this] final class Stats {
    private[this] val scope = statsReceiver.scope("optimizer")

    val latency = scope.stat("latency")

    private[this] val transitions = scope.scope("transitions")
    val up = transitions.counter("up")
    val down = transitions.counter("down")
    val idle = transitions.counter("idle")
    val adapt = transitions.counter("adapt")
    val update = transitions.counter("update")
    val noChange = transitions.counter("no_change")
    val changePhase = transitions.counter("change_phase")
    val failure = transitions.counter("failure")

    val scores = scope.stat("scores")
    val adaptNoise = scope.stat("adapt_noise")
    val adaptScore = scope.stat("adapt_score")

    private[this] val gauges = List(
      scope.addGauge("min")(min),
      scope.addGauge("max")(max),
      scope.addGauge("cliff", "current")(cliff()),
      scope.addGauge("valley", "current")(valley()))

    def remove() = gauges.foreach(_.remove())
  }
}

private[fiber_scheduler] final object Optimizer {

  private final val log = Logger()
  private final val statsScope = statsReceiver.scope("optimizer")

  private sealed trait Phase
  private final case object Up extends Phase
  private final case object Down extends Phase

  /**
   * The score function to be used by the optimizer.
   * It's important that it returns precise results
   * so the optimizer can rely on it to make decisions
   */
  case class Score(get: () => Int)
  object Score {

    /**
     * Returns the score as the delta of a continuously
     * increasing value. The returned object must be used
     * by a single optimizer and can't be shared.
     */
    def delta(get: => Long): Score =
      Score {
        var last = get
        () => {
          val n = get
          val r = (n - last).asInstanceOf[Int]
          last = n
          r
        }
      }
  }

  /**
   * Represents a limit to be respected by the optimizer.
   * Limit objects can be used by only one optimizer and can't
   * be shared.
   */
  case class Limit(get: () => Boolean, cleanup: () => Unit = () => {}) {

    /**
     * Apply another limit after the current one
     */
    def andThen(l: Limit) =
      copy(
        () => get() || l.get(),
        () => {
          cleanup()
          l.cleanup()
        })

    /**
     * Allows to define a cleanup action for when the limit
     * is detected and the optimizer reacts to it.
     * @param f
     * @return
     */
    def withCleanup(f: => Unit): Limit =
      copy(cleanup = () => {
        cleanup()
        f
      })

    /**
     * If the limit is detected multiple times, ignore it after the
     * specified number of tries
     */
    def withMaxTries(max: Int, expiration: Duration): Limit = {
      val tries = new ExpiringValue[Int](0, expiration)
      copy(get = () => {
        val t = tries()
        val r = get()
        if (t < max) {
          if (r) {
            tries.set(t + 1)
          } else {
            tries.set(0)
          }
          r
        } else {
          if (r) {
            log.info("limit ignored - reached max tries")
          }
          false
        }
      })
    }
  }

  object Limit {

    private[this] val limitScope = statsScope.scope("limit")

    /**
     * Detects a limit if the value reaches the specified limit
     */
    def ifReaches(id: String, limit: Double)(f: => Double) = {
      var last = f
      val scope = limitScope.scope(id)
      val count = scope.counter("count")
      val limitGauge = scope.addGauge("limit")(limit.toFloat)
      val valueGauge = scope.addGauge("value")(last.toFloat)

      Limit(() => {
        limitGauge.toString()
        valueGauge.toString()
        last = f
        val r = last >= limit
        if (r) {
          count.incr()
          log.info(s"reached limit: $id $last >= $limit")
        }
        r
      })
    }

    /**
     * Detects a limit if the value increases by any amount
     */
    def ifIncreases(id: String)(f: => Long): Limit = {
      var last = f
      val scope = limitScope.scope(id)
      val count = scope.counter("count")
      val valueGauge = scope.addGauge("value")(last)
      Limit(() => {
        valueGauge.toString()
        val prev = last
        last = f
        val r = last > prev
        if (r) {
          count.incr()
          log.info(s"reached limit: $id increased by ${last - prev}")
        }
        r
      })
    }
  }
}
