package com.twitter.finagle.service

import com.twitter.finagle.service.RetryPolicy.RetryableWriteException
import com.twitter.util.{Throw, Return}

object ResponseClassifier {

  /**
   * Create a [[ResponseClassifier]] with the given name for its `toString`.
   */
  def named(name: String)(underlying: ResponseClassifier): ResponseClassifier =
    new ResponseClassifier {
      def isDefinedAt(reqRep: ReqRep): Boolean = underlying.isDefinedAt(reqRep)
      def apply(reqRep: ReqRep): ResponseClass = underlying(reqRep)
      override def toString: String = name
    }

  /**
   * Finagle's default [[ResponseClassifier]].
   *
   * Finagle does not have application domain knowledge and
   * as such this treats all `Return` responses as [[ResponseClass.Success]],
   * all retryable `Throws` as [[ResponseClass.RetryableFailure]]
   * and all `Throws` as [[ResponseClass.NonRetryableFailure]].
   *
   * It is a total function covering the entire input domain and as
   * such it is recommended that it is used with user's classifiers:
   * {{{
   * theirClassier.applyOrElse(theirReqRep, ResponseClassifier.Default)
   * }}}
   */
  val Default: ResponseClassifier = named("DefaultResponseClassifier") {
    case ReqRep(_, Return(_)) => ResponseClass.Success
    case ReqRep(_, Throw(RetryableWriteException(_))) => ResponseClass.RetryableFailure
    case ReqRep(_, Throw(_)) => ResponseClass.NonRetryableFailure
  }

}
