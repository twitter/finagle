package com.twitter.finagle.http.service

import com.twitter.finagle.http.{Status, Request, Response}
import com.twitter.finagle.service.{ReqRep, ResponseClass, ResponseClassifier}
import com.twitter.util.{Return, Try}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HttpResponseClassifierTest extends FunSuite {
  private val req = Request()
  private def rep(code: Status): Try[Response] = Return(Response(code))

  test("ServerErrorsAsFailures") {
    val classifier = HttpResponseClassifier.ServerErrorsAsFailures
    assert("ServerErrorsAsFailures" == classifier.toString)

    assert(ResponseClass.NonRetryableFailure ==
      classifier(ReqRep(req, rep(Status.InternalServerError))))

    assert(!classifier.isDefinedAt(ReqRep(req, rep(Status.Ok))))
    assert(ResponseClass.NonRetryableFailure ==
      classifier.applyOrElse(
        ReqRep(req, rep(Status.InternalServerError)),
        ResponseClassifier.Default)
    )
  }

  test("apply") {
    val ok500 = HttpResponseClassifier {
      case (_, rep: Response) if rep.statusCode == 500 => ResponseClass.Success
    }
    val badReqs = HttpResponseClassifier {
      case (req: Request, _) if req.containsParam("fail") => ResponseClass.NonRetryableFailure
    }
    val classifier = ok500.orElse(badReqs)

    assert(ResponseClass.Success == classifier(ReqRep(req, rep(Status.fromCode(500)))))
    assert(ResponseClass.NonRetryableFailure ==
      classifier(ReqRep(Request("fail" -> "1"), rep(Status.Ok))))

    assert(!classifier.isDefinedAt(ReqRep(req, rep(Status.Ok))))
    assert(ResponseClass.Success ==
      classifier.applyOrElse(
        ReqRep(req, rep(Status.Ok)),
        ResponseClassifier.Default)
    )
  }

}
