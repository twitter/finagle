package com.twitter.finagle.util

import com.twitter.util.Awaitable
import java.util.concurrent.ThreadFactory

/**
 * A `java.util.concurrent.ThreadFactory` that enables blocking time tracking
 * for a given [[Runnable]].
 *
 * See:
 *
 *  - [[Awaitable.enableBlockingTimeTracking()]]
 *  - [[Awaitable.disableBlockingTimeTracking()]]
 *  - https://finagle.github.io/blog/2016/09/01/block-party/
 */
private[finagle] class BlockingTimeTrackingThreadFactory(underlying: ThreadFactory)
    extends ThreadFactory {

  def newThread(r: Runnable): Thread = {
    val wrapped = new Runnable {
      def run(): Unit = {
        Awaitable.enableBlockingTimeTracking()
        try r.run()
        finally Awaitable.disableBlockingTimeTracking()
      }
    }

    underlying.newThread(wrapped)
  }
}
