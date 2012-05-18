package com.twitter.finagle

package object stream {
  /**
   * Indicates that a stream has ended.
   */
  object EOF extends Exception
}
