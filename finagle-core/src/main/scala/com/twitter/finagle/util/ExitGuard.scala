package com.twitter.finagle.util

/**
 * ExitGuard prevents the process from exiting normally by use of a
 * nondaemon thread whenever there is at least one guarder.
 */
object ExitGuard {
  @volatile private[util] var guards: Option[(Thread, List[Guard])] = None

  final case class Guard(reason: String) {
    def unguard(): Unit = {
      ExitGuard.synchronized {
        guards match {
          case Some((thread, gs)) =>
            val newGs = gs.filterNot(_ eq this)
            guards = Some((thread, newGs))
            if (newGs.isEmpty) {
              guards = None
              thread.interrupt()
            } else updateName()
          case None => ()
        }
      }
    }
  }

  private def updateName(): Unit = {
    for ((t, gs) <- guards)
      t.setName("Finagle ExitGuard count=%d".format(gs.size))
  }

  /**
   * Prevent the process from exiting normally. You must retain the returned ExitGuard and call
   * `release` to remove the guard.
   */
  def guard(reason: String): Guard = {
    val guard = Guard(reason)
    addGuard(guard)
    guard
  }

  private[this] def addGuard(guard: Guard): Unit = {
    synchronized {
      guards match {
        case Some((thread, gs)) =>
          guards = Some((thread, guard :: gs))
        case None =>
          guards = Some((startGuardThread(), List(guard)))
      }
      updateName()
    }
  }

  def explainGuards(): String = {
    val snap = synchronized {
      guards.collect { case ((_, gs)) => gs }.getOrElse(Nil)
    }

    if (snap.isEmpty) {
      "There are no active guards."
    } else {
      s"${snap.size} active guard(s):" + snap
        .map(_.reason)
        .mkString(start = "\n", sep = "\n", end = "")
    }
  }

  private[this] def startGuardThread(): Thread = {
    new Thread {
      setDaemon(false)
      start()

      override def run(): Unit = {
        while (true) {
          try Thread.sleep(Long.MaxValue)
          catch {
            case _: InterruptedException => return
          }
        }
      }
    }
  }
}
