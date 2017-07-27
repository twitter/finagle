package com.twitter.finagle.mysql

import com.github.benmanes.caffeine.cache.Caffeine
import com.twitter.finagle.Service
import com.twitter.util.Future
import java.util.concurrent.Executor
import java.util.concurrent.LinkedBlockingQueue
import org.junit.runner.RunWith
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PrepareCacheTest extends FunSuite with Eventually with IntegrationPatience {

  test("cache prepare requests") {

    val q = new LinkedBlockingQueue[Request]()

    val stmtId = 2
    val dispatcher = Service.mk[Request, Result] { req: Request =>
      q.offer(req)
      Future.value(PrepareOK(stmtId, 1, 1, 0))
    }

    val cache = Caffeine
      .newBuilder()
      .maximumSize(11)
      .executor(new Executor { def execute(r: Runnable) = r.run() })

    val svc = new PrepareCache(dispatcher, cache)
    val r0 = PrepareRequest("SELECT 0")
    svc(r0)
    svc(r0)
    assert(q.poll() == r0)

    for (i <- 1 to 10) svc(PrepareRequest(s"SELECT $i"))
    svc(PrepareRequest("SELECT 5"))
    assert(q.contains(PrepareRequest("SELECT 5")))

    // dispatch current eldest.
    // we should maintain access order.
    svc(r0)
    assert(!q.contains(r0))
    q.clear()

    // fill cache and evict eldest.
    svc(PrepareRequest("SELECT 11"))
    q.poll()

    eventually {
      System.gc()
      assert(q.peek == CloseRequest(stmtId))
    }

    q.clear()

    // Check that the evicted element is not in cache. Caffeine evicts older
    // elements first, but its not strictly via an LRU policy. We don't actually
    // need that guarantee so it's okay to loosely check for an eviction.
    for (i <- 1 to 10) svc(PrepareRequest(s"SELECT $i"))
    assert(!q.isEmpty)
  }
}
