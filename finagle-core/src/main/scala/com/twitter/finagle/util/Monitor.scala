package com.twitter.finagle.util

import com.twitter.util.Monitor

// TODO
object DefaultMonitor extends Monitor {
  def handle(exc: Throwable) = false
}
