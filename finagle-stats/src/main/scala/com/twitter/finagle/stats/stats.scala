package com.twitter.finagle.stats

/**
 * This exists in order to allow bazel to do a few things
 * that need a scala_library target.
 */
private class stats {
  throw new IllegalStateException("Not to be constructed")
}
