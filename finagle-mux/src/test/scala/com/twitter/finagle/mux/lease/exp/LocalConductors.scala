package com.twitter.finagle.mux.lease.exp

import com.twitter.util.Local
import org.scalatest.concurrent.Conductors

trait LocalConductors extends Conductors {

  def localThread(conductor: Conductor)(fn: => Unit): Unit = {
    val outer = Local.save()
    conductor.thread {
      val saved = Local.save()
      Local.restore(outer)
      try {
        fn
      } finally {
        Local.restore(saved)
      }
    }
  }

  def localWhenFinished(conductor: Conductor)(fn: => Unit): Unit = {
    val outer = Local.save()
    conductor.whenFinished {
      val saved = Local.save()
      Local.restore(outer)
      try {
        fn
      } finally {
        Local.restore(saved)
      }
    }
  }
}
