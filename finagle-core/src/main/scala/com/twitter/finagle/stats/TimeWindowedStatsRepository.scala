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
  private[this] def repositories = Array.fill(numIntervals)(new SimpleStatsRepository)
  private[this] def currentRepository = repositories(position % numIntervals)

  timer.schedule(interval.fromNow, interval) {
    repositories((position + 1) % numIntervals) = new SimpleStatsRepository
    position += 1
  }

  def counter(description: (String, String)*) = new ReadableCounter {
    private[this] def current = currentRepository.counter(description: _*)

    def sum = repositories.foldLeft(0) { (total, repository) =>
      total + repository.counter(description: _*).sum
    }

    def incr(delta: Int) = current.incr(delta)
  }

  def gauge(description: (String, String)*) = new ReadableGauge {
    private[this] def current = currentRepository.gauge(description: _*)

    def summary = repositories.foldLeft(Summary(0.0f, 0)) { (acc, repository) =>
      val summary = repository.gauge(description: _*).summary
      Summary(acc.total + summary.total, acc.count + summary.count)
    }

    def measure(value: Float) = current.measure(value)
  }

  def mkGauge(description: Seq[(String, String)], f: => Float) {
    timer.schedule(Time.now, interval)(f)
  }
}