package com.twitter.finagle.util

import org.specs.Specification
import org.specs.mock.Mockito

object AsyncSemaphoreSpec extends Specification with Mockito {
  "AsyncSemaphore" should {
    val f = mock[Function0[Unit]]
    val s = new AsyncSemaphore(2)

    "execute immediately while permits are available" in {
      s.acquire(f())
      there was one(f)()
      s.acquire(f())
      there were two(f)()

      s.acquire(f())
      there were two(f)()
    }

    "execute deferred computations when permits are released" in {
      s.acquire(f())
      s.acquire(f())
      s.acquire(f())
      s.acquire(f())

      there were two(f)()

      s.release()
      there were three(f)()

      s.release()
      there were 4.times(f)()

      s.release()
      there were 4.times(f)()
    }
  }
}
