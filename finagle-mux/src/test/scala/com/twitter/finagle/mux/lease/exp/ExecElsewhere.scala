package com.twitter.finagle.mux.lease.exp

import com.twitter.util.Local
import org.scalatest.concurrent.Eventually
import org.scalatest.FunSuite

trait ExecElsewhere extends FunSuite with Eventually {
  def exec(setup: () => Unit, nexts: () => Unit*) {
    val saved = Local.save()
    val runnable = new Runnable {
      def run() {
        Local.restore(saved)
        setup()
      }
    }
    val t = new Thread(runnable)
    t.start()

    nexts foreach { next =>
      eventually {
        assert(t.getState === Thread.State.TIMED_WAITING)
      }
      next()
    }

    t.join()
    assert(t.isAlive === false)
  }
}
