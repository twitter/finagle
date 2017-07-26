package com.twitter.finagle.service

import scala.util.control.NoStackTrace

/**
 * Used by [[com.twitter.finagle.param.ResponseClassifier response classification]]
 * to indicate synthetic failures that are not `Exceptions`.
 *
 * @see [[com.twitter.finagle.service.StatsFilter]]
 * @see The [[https://twitter.github.io/finagle/guide/FAQ.html#what-is-a-com-twitter-finagle-service-responseclassificationsyntheticexception FAQ]]
 *      for more details.
 */
class ResponseClassificationSyntheticException private[finagle] ()
    extends Exception
    with NoStackTrace {
  override def getMessage: String =
    "A synthetic ResponseClassification failure"
}
