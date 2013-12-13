package com.twitter.finagle.service

import RetryPolicy._
import com.twitter.conversions.time._
import com.twitter.finagle.{CancelledRequestException, TimeoutException, WriteException}
import com.twitter.util._
import org.junit.runner.RunWith
import org.scalatest.FunSpec
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RetryPolicyTest extends FunSpec {
  describe("RetryPolicy") {
    val NoExceptions: PartialFunction[Try[Nothing], Boolean] = {
      case _ => false
    }
    val timeoutExc = new TimeoutException {
      protected val timeout = 0.seconds
      protected val explanation = "!"
    }

    it("should WriteExceptionsOnly") {
      val weo = WriteExceptionsOnly orElse NoExceptions

      assert(weo(Throw(new Exception)) === false)
      assert(weo(Throw(WriteException(new Exception))) === true)
      assert(weo(Throw(WriteException(new CancelledRequestException))) === false)
      assert(weo(Throw(timeoutExc)) === false)
    }

    it("should TimeoutAndWriteExceptionsOnly") {
      val weo = TimeoutAndWriteExceptionsOnly orElse NoExceptions

      assert(weo(Throw(new Exception)) === false)
      assert(weo(Throw(WriteException(new Exception))) === true)
      assert(weo(Throw(WriteException(new CancelledRequestException))) === false)
      assert(weo(Throw(timeoutExc)) === true)
    }  }
}
