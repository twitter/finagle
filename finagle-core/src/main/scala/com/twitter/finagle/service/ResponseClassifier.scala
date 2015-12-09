package com.twitter.finagle.service

import com.twitter.finagle.service.RetryPolicy.RetryableWriteException
import com.twitter.util.{Throw, Return}

object ResponseClassifier {

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
  val Default: ResponseClassifier = {
    case ReqRep(_, Return(_)) => ResponseClass.Success
    case ReqRep(_, Throw(RetryableWriteException(_))) => ResponseClass.RetryableFailure
    case ReqRep(_, Throw(_)) => ResponseClass.NonRetryableFailure
  }

}
