package com.twitter.finagle.stats

import com.twitter.util.{FuturePool, Future}
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite

class NonReentrantReadWriteLockTest extends AnyFunSuite with Eventually {
  test("Read locks block write locks (concurrent)") {
    val lock = new NonReentrantReadWriteLock()
    lock.acquireShared(1)
    val f: Future[Unit] = FuturePool.unboundedPool {
      lock.acquire(1)
      lock.release(1)
      ()
    }

    assert(!f.isDefined)
    lock.releaseShared(1)
    eventually {
      assert(f.isDefined)
    }
  }

  test("Read locks block write locks (single threaded)") {
    val lock = new NonReentrantReadWriteLock()
    assert(lock.tryAcquireShared(1) > 0)
    assert(!lock.tryAcquire(1))
  }

  test("Write locks block read locks (concurrent)") {
    val lock = new NonReentrantReadWriteLock()
    lock.acquire(1)
    val f: Future[Unit] = FuturePool.unboundedPool {
      lock.acquireShared(1)
      lock.releaseShared(1)
      ()
    }

    assert(!f.isDefined)
    lock.release(1)
    eventually {
      assert(f.isDefined)
    }
  }

  test("Write locks block read locks (single threaded)") {
    val lock = new NonReentrantReadWriteLock()
    assert(lock.tryAcquire(1))
    assert(!(lock.tryAcquireShared(1) > 0))
  }

  test("Supports multiple read locks (concurrent)") {
    val lock = new NonReentrantReadWriteLock()
    lock.acquireShared(1)
    val f: Future[Unit] = FuturePool.unboundedPool {
      lock.acquireShared(1)
      ()
    }
    eventually {
      assert(f.isDefined)
    }
  }

  test("Supports multiple read locks (single threaded)") {
    val lock = new NonReentrantReadWriteLock()
    assert(lock.tryAcquireShared(1) > 0)
    assert(lock.tryAcquireShared(1) > 0)
  }

  test("Does not support multiple write locks (concurrent)") {
    val lock = new NonReentrantReadWriteLock()
    lock.acquire(1)
    val f: Future[Unit] = FuturePool.unboundedPool {
      lock.acquire(1)
      ()
    }
    assert(!f.isDefined)
    lock.release(1)
    eventually {
      assert(f.isDefined)
    }
  }

  test("Does not support multiple write locks (single threaded)") {
    val lock = new NonReentrantReadWriteLock()
    assert(lock.tryAcquire(1))
    assert(!lock.tryAcquire(1))
  }
}
