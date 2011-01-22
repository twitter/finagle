package com.twitter.finagle.util

import com.twitter.util.Future

/**
 * Manage object lifecycle [eg. in a pool]. Specifically: creation,
 * destruction & health checking.
 */
trait LifecycleFactory[A] {
  /**
   * Create a new item. This call cannot block. Instead return an
   * (asynchronous) Future.
   */
  def make(): Future[A]

  /**
   * The given item has been end-of-life'd.
   */
  def dispose(item: A): Unit

  /**
   * Query the health of the given item.
   */
  def isHealthy(item: A): Boolean
}


