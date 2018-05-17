package com.twitter.finagle.service

import com.twitter.finagle.service.RetryPolicy.RetryableWriteException
import com.twitter.finagle.{ChannelClosedException, Failure, TimeoutException}
import com.twitter.util.{Throw, TimeoutException => UtilTimeoutException, Return}

object ResponseClassifier {

  /**
   * Create a [[ResponseClassifier]] with the given name for its `toString`.
   *
   * @note be careful when `underlying` composes other `ResponseClassifiers`
   *       which are not total.
   */
  def named(name: String)(underlying: ResponseClassifier): ResponseClassifier =
    new ResponseClassifier {
      def isDefinedAt(reqRep: ReqRep): Boolean = underlying.isDefinedAt(reqRep)
      def apply(reqRep: ReqRep): ResponseClass = underlying(reqRep)
      override def toString: String = name

      override def orElse[A1 <: ReqRep, B1 >: ResponseClass](
        that: PartialFunction[A1, B1]
      ): PartialFunction[A1, B1] = {
        val orElsed = super.orElse(that).asInstanceOf[ResponseClassifier]
        named(s"$toString.orElse($that)")(orElsed)
          .asInstanceOf[PartialFunction[ReqRep, ResponseClass]]
      }
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
   * theirClassifier.applyOrElse(theirReqRep, ResponseClassifier.Default)
   * }}}
   */
  val Default: ResponseClassifier = named("DefaultResponseClassifier") {
    case ReqRep(_, Return(_)) => ResponseClass.Success
    case ReqRep(_, Throw(RetryableWriteException(_))) => ResponseClass.RetryableFailure
    case ReqRep(_, Throw(_)) => ResponseClass.NonRetryableFailure
  }

  /**
   * Implementation for the [[ResponseClassifier]] that retries requests on all throws.
   *
   * This would be useful for instances of idempotent requests, for example
   * on database reads or similar.
   */
  val RetryOnThrows: ResponseClassifier = named("RetryOnThrowsResponseClassifier") {
    case ReqRep(_, Throw(_)) => ResponseClass.RetryableFailure
  }

  /**
   *  Implementation for the [[ResponseClassifier]] that retries requests on all timeout
   *  exceptions.
   *
   *  This would be useful for instances of idempotent requests, for example
   *  on database reads or similar.  May also be useful for non-idempotent requests
   *  depending on how the remote service handles duplicate requests.
   */
  val RetryOnTimeout: ResponseClassifier = named("RetryOnTimeoutClassifier") {
    case ReqRep(_, Throw(Failure(Some(_: TimeoutException)))) =>
      ResponseClass.RetryableFailure
    case ReqRep(_, Throw(Failure(Some(_: UtilTimeoutException)))) =>
      ResponseClass.RetryableFailure
    case ReqRep(_, Throw(_: TimeoutException)) => ResponseClass.RetryableFailure
    case ReqRep(_, Throw(_: UtilTimeoutException)) => ResponseClass.RetryableFailure
  }

  /**
   *  Implementation for the [[ResponseClassifier]] that retries requests on all channel
   *  closed exceptions.
   *
   *  This is safe to use for idempotent requests.
   */
  val RetryOnChannelClosed: ResponseClassifier = named("RetryOnChannelClosedClassifier") {
    case ReqRep(_, Throw(_: ChannelClosedException)) => ResponseClass.RetryableFailure
  }
}
