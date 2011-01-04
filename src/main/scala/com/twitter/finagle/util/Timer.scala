package com.twitter.finagle.util

import java.util.concurrent.TimeUnit

import org.jboss.netty.util.{Timer, TimerTask, Timeout}

import com.twitter.util.Duration

class RichTimer(val self: Timer) {
  def apply(after: Duration)(f: => Unit): Timeout = {
    self.newTimeout(new TimerTask {
      def run(to: Timeout) {
        if (!to.isCancelled())
          f
      }
    }, after.inMilliseconds, TimeUnit.MILLISECONDS)
  }
}
