package com.twitter.finagle.http.service

import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.service.{ResponseClass, ReqRep, ResponseClassifier}
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
   */
  val ServerErrorsAsFailures: ResponseClassifier =
    ResponseClassifier.named("ServerErrorsAsFailures") {
      case ReqRep(_, Return(r: Response)) if is500(r) => ResponseClass.NonRetryableFailure
    }

  /**
   * Converts from the more natural `(http.Request, http.Response)` types
   * to a [[ResponseClassifier]].
   */
  def apply(
    underlying: PartialFunction[(Request, Response), ResponseClass]
  ): ResponseClassifier = new ResponseClassifier {
    def isDefinedAt(x: ReqRep): Boolean = x match {
      case ReqRep(req: Request, Return(rep: Response)) => underlying.isDefinedAt((req, rep))
      case _ => false
    }

    def apply(x: ReqRep): ResponseClass = x match {
      case ReqRep(req: Request, Return(rep: Response)) => underlying((req, rep))
    }
  }
}
