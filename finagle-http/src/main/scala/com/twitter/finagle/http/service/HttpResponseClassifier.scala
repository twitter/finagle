package com.twitter.finagle.http.service

import com.twitter.finagle.http.filter.HttpNackFilter
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.service.{ReqRep, ResponseClass, ResponseClassifier}
import com.twitter.util.Return

/**
 * [[ResponseClassifier ResponseClassifiers]] for use with `finagle-http`
 * request/responses which are [[Request]]/[[Response]] typed.
 */
object HttpResponseClassifier {
  private[this] def is500(r: Response): Boolean =
    r.statusCode >= 500 && r.statusCode <= 599

  /**
   * Categorizes responses with status codes in the 500s as
   * [[ResponseClass.NonRetryableFailure NonRetryableFailures]].
   *
   * @note that retryable nacks are categorized as a [[ResponseClass.RetryableFailure]].
   */
  val ServerErrorsAsFailures: ResponseClassifier =
    ResponseClassifier.named("ServerErrorsAsFailures") {
      case ReqRep(_, Return(r: Response)) if is500(r) =>
        if (HttpNackFilter.isRetryableNack(r))
          ResponseClass.RetryableFailure
        else
          ResponseClass.NonRetryableFailure
    }

  /**
   * Converts from the more natural `(http.Request, http.Response)` types
   * to a [[ResponseClassifier]].
   */
  def apply(underlying: PartialFunction[(Request, Response), ResponseClass]): ResponseClassifier =
    new ResponseClassifier {
      override def toString: String =
        s"HttpResponseClassifier($underlying)"

      def isDefinedAt(x: ReqRep): Boolean = x match {
        case ReqRep(req: Request, Return(rep: Response)) => underlying.isDefinedAt((req, rep))
        case _ => false
      }

      def apply(x: ReqRep): ResponseClass = x match {
        case ReqRep(req: Request, Return(rep: Response)) => underlying((req, rep))
        case _ => throw new AssertionError(s"$this applied to $x")
      }
    }
}
