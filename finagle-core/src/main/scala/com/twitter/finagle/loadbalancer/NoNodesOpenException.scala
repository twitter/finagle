package com.twitter.finagle.loadbalancer

import com.twitter.finagle.{FailureFlags, SourcedException}
import com.twitter.logging.{HasLogLevel, Level}

/**
 * While this exception is safe to retry, the assumption used here is
 * that the underlying situation will not change soon enough to make
 * a retry worthwhile as retrying is most likely to eat up the entire
 * budget.
 */
class NoNodesOpenException private[loadbalancer] (val flags: Long)
    extends RuntimeException
    with FailureFlags[NoNodesOpenException]
    with HasLogLevel
    with SourcedException {

  protected def copyWithFlags(newFlags: Long): NoNodesOpenException =
    new NoNodesOpenException(newFlags)

  def logLevel: Level = Level.DEBUG
}
