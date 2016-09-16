package com.twitter.finagle.http2.param

import com.twitter.finagle.Stack

/**
 * A class eligible for configuring whether to use the http/2 "prior knowledge"
 * protocol or not.
 *
 * Note that both client and server must be configured for prior knowledge.
 */
case class PriorKnowledge(enabled: Boolean) {
  def mk(): (PriorKnowledge, Stack.Param[PriorKnowledge]) =
    (this, PriorKnowledge.param)
}

object PriorKnowledge {
  implicit val param = Stack.Param(PriorKnowledge(false))
}
