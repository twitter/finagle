package com.twitter.finagle.stats

import java.util.concurrent.locks.AbstractQueuedSynchronizer
import scala.annotation.tailrec

/**
 * This is a cheaper read-write lock than the ReentrantReadWriteLock.
 *
 * It does even less under the hood than ReentrantReadWriteLock, and is intended
 * for use on the hot path.
 *
 * To acquire the write lock, call NonReentrantReadWriteLock#acquire, and to release, call
 * NonReentrantReadWriteLock#release.  The argument that's passed in is unused.
 *
 * To acquire the read lock, call NonReentrantReadWriteLock#acquireShared, and to release,
 * call NonReentrantReadWriteLock#releaseShared.  The argument that's passed in is unused.
 *
 * Note that this lock is non-reentrant, non-fair, and you can't acquire the
 * write lock while holding the read lock.  Under constant read use, writers may
 * starve, so this is better for bursty read workloads.
 */
private[stats] final class NonReentrantReadWriteLock extends AbstractQueuedSynchronizer {
  // we haven't provided an implementation for isHeldExclusively because the
  // docs for AQS advise implementors to provide implementations "as applicable"
  // and our use case doesn't require knowing when the lock is held exclusively.

  override def tryAcquireShared(num: Int): Int = {
    val cur = getState()
    if (cur >= 0 && compareAndSetState(cur, cur + 1)) 1 else -1
  }

  @tailrec
  override def tryReleaseShared(num: Int): Boolean = {
    val cur = getState()
    if (compareAndSetState(cur, cur - 1)) true
    else tryReleaseShared(num)
  }

  override def tryAcquire(num: Int): Boolean = {
    val cur = getState()
    cur == 0 && compareAndSetState(0, -1)
  }
  override def tryRelease(num: Int): Boolean = {
    setState(0)
    true
  }
}
