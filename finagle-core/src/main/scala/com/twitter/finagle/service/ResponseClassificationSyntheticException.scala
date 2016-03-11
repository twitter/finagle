package com.twitter.finagle.service

import com.twitter.util.NoStacktrace

/**
 * Used by [[com.twitter.finagle.param.ResponseClassifier response classification]]
 * to indicate synthetic failures that are not `Exceptions`.
 *
 * @see [[com.twitter.finagle.service.StatsFilter]]
 */
class ResponseClassificationSyntheticException private[finagle]()
  extends Exception
  with NoStacktrace {
  override def getMessage: String =
    "A synthetic ResponseClassification failure"
}
