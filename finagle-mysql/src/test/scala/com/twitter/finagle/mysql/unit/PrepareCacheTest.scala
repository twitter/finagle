package com.twitter.finagle.mysql

import com.github.benmanes.caffeine.cache.Caffeine
import com.twitter.finagle.Service
import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver}
import com.twitter.util.Future
import java.util.concurrent.Executor
import java.util.concurrent.LinkedBlockingQueue
import org.scalatest.funsuite.AnyFunSuite

class PrepareCacheTest extends AnyFunSuite {

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

    val svc = new PrepareCache(dispatcher, cache, NullStatsReceiver)
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

    assert(q.peek == CloseRequest(stmtId))

    q.clear()

    // Check that the evicted element is not in cache. Caffeine evicts older
    // elements first, but it's not strictly via an LRU policy. We don't actually
    // need that guarantee so it's okay to loosely check for an eviction.
    for (i <- 1 to 10) svc(PrepareRequest(s"SELECT $i"))
    assert(!q.isEmpty)
  }

  test("emit stats") {

    val q = new LinkedBlockingQueue[Request]()

    val stmtId = 2
    val dispatcher = Service.mk[Request, Result] { req: Request =>
      q.offer(req)
      Future.value(PrepareOK(stmtId, 1, 1, 0))
    }

    val cache = Caffeine
      .newBuilder()
      .maximumSize(1)
      .executor(new Executor { def execute(r: Runnable) = r.run() })

    val ist = new InMemoryStatsReceiver()
    val svc = new PrepareCache(dispatcher, cache, ist)
    val r0 = PrepareRequest("SELECT 0")
    svc(r0)
    svc(r0)
    assert(q.poll() == r0)

    assert(ist.counters(Seq("pstmt-cache", "calls")) == 2)
    assert(ist.counters(Seq("pstmt-cache", "misses")) == 1)
    assert(!ist.counters.contains(Seq("pstmt-cache", "evicted_size")))

    val r1 = PrepareRequest("SELECT 1")
    svc(r1)

    val expectedCounters = Seq(
      Seq("pstmt-cache", "calls") -> 3,
      Seq("pstmt-cache", "misses") -> 2,
      Seq("pstmt-cache", "evicted_size") -> 1
    ).toMap

    assert(expectedCounters == ist.counters)
  }

}
