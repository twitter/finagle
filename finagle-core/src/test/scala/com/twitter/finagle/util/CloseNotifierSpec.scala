package com.twitter.finagle.util

import com.twitter.util.Promise
import org.specs.SpecificationWithJUnit

class CloseNotifierSpec extends SpecificationWithJUnit {
  "CloseNotifier" should {
    "invoke onClose handlers in reverse order of adding" in {
      val closing = new Promise[Unit]
      val notifier = CloseNotifier.makeLifo(closing)
      var invocations: List[Int] = Nil

      (1 to 10).foreach { i =>
        notifier.onClose { invocations ::= i }
      }

      closing.setValue(())
      invocations must be_==((1 to 10).toList)
    }

    "invoke onClose handler immediately if close event already happened" in {
      val closing = new Promise[Unit]
      val notifier = CloseNotifier.makeLifo(closing)

      closing.setValue(())
      var invoked = false
      notifier.onClose { invoked = true }

      invoked must beTrue
    }
  }
}
