package com.twitter.finagle.util

/**
 * ExitGuard prevents the process from exiting normally by use of a
 * nondaemon thread whenever there is at least one guarder.
 */
object ExitGuard {
  private var pending: Option[(Int, Thread)] = None

  private def updateName() {
    for ((n, t) <- pending)
      t.setName("Finagle ExitGuard count=%d".format(n))
  }

  /** Prevent the process from exiting normally */
  def guard(): Unit = synchronized {
    pending = pending match {
      case None =>
        val t = new Thread {
          setDaemon(false)
          start()

          override def run() {
            while (true) {
              try Thread.sleep(Long.MaxValue) catch {
                case _: InterruptedException => return
              }
            }
          }
        }
        Some(1, t)
      case Some((n, t)) => 
        Some((n+1, t))
    }
    updateName()
  }

  /** Undo a call to guard */
  def unguard(): Unit = synchronized {
    pending = pending match {
      case None => throw new IllegalStateException("unguard() called too many times")
      case Some((1, t)) =>
        t.interrupt()
        t.join()
        None
      case Some((n, t)) =>
        Some((n-1, t))
    }
    updateName()
  }
}
