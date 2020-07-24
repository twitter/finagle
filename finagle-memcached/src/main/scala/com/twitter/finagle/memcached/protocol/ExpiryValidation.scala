package com.twitter.finagle.memcached.protocol

import com.twitter.logging.{Level, Logger}
import com.twitter.util.Time

private[memcached] object ExpiryValidation {
  private val log = Logger.get()

  /**
   * Checks if expiry is valid
   */
  def checkExpiry(command: String, expiry: Time): Boolean = {
    // Item never expires if expiry is Time.epoch
    if (expiry == Time.epoch) true
    else if (expiry < Time.now) {
      if (log.isLoggable(Level.TRACE))
        log.trace(s"Negative expiry for $command: item will expire immediately")
      false
    } else true
  }
}
