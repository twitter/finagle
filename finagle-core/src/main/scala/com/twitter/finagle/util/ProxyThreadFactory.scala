package com.twitter.finagle.util

import java.util.concurrent.ThreadFactory

private[finagle] object ProxyThreadFactory {

  /**
   * @param preRun run before `Runnable.run`
   * @param postRun run after `Runnable.run` regardless of if
   *                it completes successfully or throws an exception.
   */
  def newProxiedRunnable(
    preRun: () => Unit,
    postRun: () => Unit
  ): Runnable => Runnable = { r: Runnable =>
    new Runnable {
      def run(): Unit = {
        preRun()
        try r.run()
        finally postRun()
      }
    }
  }
}

/**
 * A `java.util.concurrent.ThreadFactory` which gives proxies
 * `ThreadFactory.newThread` and also allows the `Runnable`
 * passed in to be manipulated.
 */
private[finagle] class ProxyThreadFactory(
    underlying: ThreadFactory,
    newRunnable: Runnable => Runnable)
  extends ThreadFactory {

  def newThread(r: Runnable): Thread = {
    val newR = newRunnable(r)
    underlying.newThread(newR)
  }
}
