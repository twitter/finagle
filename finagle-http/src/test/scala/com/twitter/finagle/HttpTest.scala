package com.twitter.finagle

import com.twitter.finagle.service.{ResponseClass, ResponseClassifier}
import com.twitter.finagle.toggle.flag
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HttpTest extends FunSuite {

  private def classifier(params: Stack.Params): ResponseClassifier =
    params[param.ResponseClassifier].responseClassifier

  test("client uses custom response classifier when specified") {
    flag.overrides.let(Http.ServerErrorsAsFailuresToggleId, 1.0) {
      val customRc: ResponseClassifier = {
        case _ => ResponseClass.Success
      }

      val client = new Http.Client().withResponseClassifier(customRc)
      val rc = classifier(client.params)
      assert(rc == customRc)
    }
  }

  test("server uses custom response classifier when specified") {
    flag.overrides.let(Http.ServerErrorsAsFailuresToggleId, 1.0) {
      val customRc: ResponseClassifier = {
        case _ => ResponseClass.Success
      }

      val client = new Http.Server().withResponseClassifier(customRc)
      val rc = classifier(client.params)
      assert(rc == customRc)
    }
  }

}
