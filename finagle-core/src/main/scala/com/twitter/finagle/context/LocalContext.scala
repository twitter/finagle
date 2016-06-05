package com.twitter.finagle.context

/**
 * A type of context that is local to the process. The type of Key is
 * also unique (generative) to each instance of this context, so that keys
 * cannot be used across different instances of this context type.
 */
class LocalContext extends Context {
  class Key[A]

  /**
   * A java-friendly key constructor.
   */
  def newKey[A]() = new Key[A]
}
