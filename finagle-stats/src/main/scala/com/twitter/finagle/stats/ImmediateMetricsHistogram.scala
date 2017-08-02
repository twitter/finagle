package com.twitter.finagle.stats

object ImmediateMetricsHistogram {

  /**
   * Constructs a [[MetricsHistogram]] that has no buffering, windowing, or latching.
   *
   * Any value added is immediately aggregated in the result.
   * Useful for tests or short-lived jobs.
   */
  def apply(name: String, quantiles: IndexedSeq[Double]): MetricsHistogram = {
    new MetricsHistogram {
      private[this] val stats = BucketedHistogram()

      def snapshot(): Snapshot = synchronized {
        new Snapshot {
          def average: Double = stats.average
          def count: Long = stats.count
          def min: Long = stats.minimum
          def max: Long = stats.maximum
          def sum: Long = stats.sum

          def percentiles: IndexedSeq[Snapshot.Percentile] = {
            stats.getQuantiles(quantiles).zip(quantiles).map {
              case (q, p) =>
                Snapshot.Percentile(p, q)
            }
          }
        }
      }

      def clear(): Unit = synchronized {
        stats.clear()
      }

      def add(n: Long): Unit = synchronized {
        stats.add(n)
      }
    }
  }
}
