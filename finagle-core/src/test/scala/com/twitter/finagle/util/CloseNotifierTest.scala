package com.twitter.finagle.util

import com.twitter.util.Promise
import org.scalatest.funsuite.AnyFunSuite

class CloseNotifierTest extends AnyFunSuite {

  test("CloseNotifier should invoke onClose handlers in reverse order of adding") {
    val closing = new Promise[Unit]
    val notifier = CloseNotifier.makeLifo(closing)
    var invocations: List[Int] = Nil

    (1 to 10).foreach { i =>
      notifier.onClose {
        invocations ::= i
      }
    }

    closing.setDone()
    assert(invocations == (1 to 10).toList)
  }

  test("CloseNotifier should invoke onClose handler immediately if close event already happened") {
    val closing = new Promise[Unit]
    val notifier = CloseNotifier.makeLifo(closing)

    closing.setDone()
    var invoked = false
    notifier.onClose {
      invoked = true
    }

    assert(invoked)
  }
}
