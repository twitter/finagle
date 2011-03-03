package com.twitter.finagle.util

import org.specs.Specification
import org.specs.mock.Mockito
import java.util.concurrent.ConcurrentLinkedQueue

object AsyncSemaphoreSpec extends Specification with Mockito {
  "AsyncSemaphore" should {
    val f = mock[() => Unit]
    val s = new AsyncSemaphore(2)
    val permits = new ConcurrentLinkedQueue[AsyncSemaphore#Permit]
    def acquire() {
      s.acquire() onSuccess { permit =>
        f()
        permits add permit
      }
    }

    "execute immediately while permits are available" in {
      acquire()
      there was one(f)()

      acquire()
      there were two(f)()

      acquire()
      there were two(f)()
    }

    "execute deferred computations when permits are released" in {
      acquire()
      acquire()
      acquire()
      acquire()

      there were two(f)()

      permits.poll().release()
      there were three(f)()

      permits.poll().release()
      there were 4.times(f)()

      permits.poll().release()
      there were 4.times(f)()
    }
  }
}
