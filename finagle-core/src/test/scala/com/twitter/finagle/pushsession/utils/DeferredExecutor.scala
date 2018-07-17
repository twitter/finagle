package com.twitter.finagle.pushsession.utils

import java.util
import java.util.concurrent.Executor

class DeferredExecutor extends Executor {
  private[this] val queue = new util.ArrayDeque[Runnable]()

  def execute(command: Runnable): Unit = queue.add(command)

  def pendingTasks: Int = queue.size

  /** Execute all pending messages, including those queued during a previous execution */
  def executeAll(): Unit = {
    while (!queue.isEmpty) {
      queue.poll().run()
    }
  }
}
