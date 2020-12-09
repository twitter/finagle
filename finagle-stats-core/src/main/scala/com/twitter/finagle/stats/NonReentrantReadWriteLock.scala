package com.twitter.finagle.stats

import java.util.concurrent.locks.AbstractQueuedSynchronizer
import scala.annotation.tailrec

import com.twitter.app.GlobalFlag

object rwLockSpinWait
// Defaults to -1 which allows the NonReentrantReadWriteLock class to
// set a system specific default.
    extends GlobalFlag[Int](
      -1,
      """Experimental flag.  Control how many times NonReentrantReadWriteLock 
      | retries an acquire before executing its slow path acquire.  Default
      | value of -1 lets the system decide how long to spin""".stripMargin)

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
  private val SpinMax: Int = if (rwLockSpinWait() == -1) {
    // Default number of spins for aarch64 machines set to 5 by default.
    // See below comment on reason.
    if (System.getProperty("os.arch") == "aarch_64") 5
    else 1
  } else rwLockSpinWait()

  override def tryAcquireShared(num: Int): Int = {
    tryAcquireShared(getState(), 1)
  }

  // Try to acquire the lock up to SpinMax times before failing and
  // allowing AbstractQueuedSynchronizer to execute the slow-path lock.
  // This is important on Arm systems where compareAndSetState may fail
  // more often due to its weaker memory model.
  @tailrec
  private[this] def tryAcquireShared(cur: Int, tries: Int): Int = {
    if (cur >= 0 && compareAndSetState(cur, cur + 1)) 1
    // When JDK8 support is deprecated, Thread.onSpinWait() should be
    // used in the else if clause to avoid flooding the memory
    // system with compareAndSetState in a tight loop.
    else if (tries < SpinMax) tryAcquireShared(getState(), tries + 1)
    else -1
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
