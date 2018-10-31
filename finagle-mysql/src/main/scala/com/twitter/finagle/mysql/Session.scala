package com.twitter.finagle.mysql

import com.twitter.util.Future

/**
 * Explicitly manage the lifecycle of a MySQL session.
 *
 * @see Client#session()
 */
trait Session {

  /**
   * Discard the session. The connection will become unusable and will not be returned to the pool.
   * This can be used when an error is detected at the application level that signifies that the
   * session is no longer usable or that the server state is unknown or inconsistent.
   */
  def discard(): Future[Unit]
}
