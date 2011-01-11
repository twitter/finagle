package com.twitter.finagle.stats

import com.twitter.util.{Timer, Duration, JavaTimer, Time}

/**
 * A StatsRepository that keeps a rolling set of windows of data. Stats are
 * collected over a time window, and a limited number of time windows are
 * stored. Older windows drop off the back, newer windows are appended to
 * the front.
 *
 * @param  interval  the duration of an individual time window
 * @param  windows   the number of time windows to keep around
 * @param  timer     a timer to schedule creating and dropping time windows
 */
class TimeWindowedStatsRepository(numIntervals: Int, interval: Duration, timer: Timer = new JavaTimer)
  extends StatsRepository
{
  @volatile private[this] var position = 0
  private[this] def repositories = new Array[StatsRepository](numIntervals)
  private[this] def currentRepository = repositories(position % numIntervals)

  repositories(0) = new SimpleStatsRepository
  timer.schedule(interval.fromNow, interval) {
    repositories((position + 1) % numIntervals) = new SimpleStatsRepository
    position += 1
  }

  private[this] class Counter(path: (String, String)*) extends OCounter {
    private[this] def current = currentRepository.counter(path: _*)

    def sum = repositories.foldLeft(0) { (total, repository) =>
      total + repository.counter(path: _*).sum
    }

    def incr(delta: Int) = current.incr(delta)
  }

  private[this] class Gauge(path: (String, String)*) extends OGauge {
    private[this] def current = currentRepository.gauge(path: _*)

    def summary = repositories.foldLeft(Summary(0.0f, 0)) { (acc, repository) =>
      val summary = repository.gauge(path: _*).summary
      Summary(acc.total + summary.total, acc.count + summary.count)
    }

    def measure(value: Float) = current.measure(value)
  }

  def counter(path: (String, String)*): OCounter = new Counter(path: _*)
  def gauge(path: (String, String)*): OGauge = new Gauge(path: _*)
  def mkGauge(description: Seq[(String, String)], f: => Float) {
    timer.schedule(Time.now, interval)(f)
  }
}