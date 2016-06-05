package com.twitter.finagle.thrift.service

import com.twitter.finagle.context.Contexts
import com.twitter.finagle.service.{ResponseClassifier, ResponseClass, ReqRep}
import com.twitter.finagle.service.ResponseClass._
import com.twitter.finagle.thrift.DeserializeCtx
import com.twitter.finagle.thrift.thriftscala.{InvalidQueryException, Echo}
import com.twitter.io.Charsets
import com.twitter.util.{Return, Throw}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ThriftResponseClassifierTest extends FunSuite {

  private val classifier = ThriftResponseClassifier.usingDeserializeCtx {
    case ReqRep(_, Return(rep: String)) if rep == "nope" => RetryableFailure
    case ReqRep(_, Throw(e: InvalidQueryException)) if e.errorCode == 4 => NonRetryableFailure
    case ReqRep(Echo.Echo.Args(in), _) if in == "lol" => NonRetryableFailure
  }

  private val deserializer = { bytes: Array[Byte] =>
    val asString = new String(bytes, Charsets.Utf8)
    if (asString.startsWith("fail")) Throw(new InvalidQueryException(asString.length))
    else Return(asString)
  }

  test("usingDeserializeCtx basics") {
    def testApply(in: String, expectedClass: ResponseClass): Unit = {
      val ctx = new DeserializeCtx(Echo.Echo.Args(in), deserializer)
      Contexts.local.let(DeserializeCtx.Key, ctx) {
        val rep = in.getBytes(Charsets.Utf8)
        assert(expectedClass == classifier(ReqRep(in, Return(rep))))
      }
    }
    testApply("nope", RetryableFailure)
    testApply("lol", NonRetryableFailure)
    testApply("fail", NonRetryableFailure)

    def testApplyOrElse(in: String, expectedClass: ResponseClass): Unit = {
      val ctx = new DeserializeCtx(Echo.Echo.Args(in), deserializer)
      Contexts.local.let(DeserializeCtx.Key, ctx) {
        val rep = in.getBytes(Charsets.Utf8)
        assert(!classifier.isDefinedAt(ReqRep(in, Return(rep))))
        assert(expectedClass ==
          classifier.applyOrElse(ReqRep(in, Return(rep)), ResponseClassifier.Default))
      }
    }
    testApplyOrElse("yep", Success)
    testApplyOrElse("failing", Success) // falls through to the default classifier
  }

  test("usingDeserializeCtx ignores exceptions during deserialization") {
    val toThrow = new RuntimeException("welp")
    val throwingDeser = { bytes: Array[Byte] =>
      throw toThrow
    }

    val input = "throw"
    val ctx = new DeserializeCtx(Echo.Echo.Args(input), throwingDeser)
    Contexts.local.let(DeserializeCtx.Key, ctx) {
      val rep = input.getBytes(Charsets.Utf8)

      assert(!classifier.isDefinedAt(ReqRep(input, Return(rep))))
      assert(Success ==
        classifier.applyOrElse(ReqRep(input, Return(rep)), ResponseClassifier.Default))
    }
  }

  test("usingDeserializeCtx handles no DeserializationCtx") {
    def testApply(in: String, expectedClass: ResponseClass): Unit = {
      val rep = in.getBytes(Charsets.Utf8)
      assert(expectedClass ==
        classifier.applyOrElse(ReqRep(in, Return(rep)), ResponseClassifier.Default))
    }
    testApply("nope", Success)
    testApply("lol", Success)
    testApply("fail", Success)
  }

  test("ThriftExceptionsAsFailures") {
    import ThriftResponseClassifier.{ThriftExceptionsAsFailures, usingDeserializeCtx}

    val classifier = usingDeserializeCtx(ThriftExceptionsAsFailures)
    assert("Thrift.usingDeserializeCtx(ThriftExceptionsAsFailures)" == classifier.toString())

    def testApply(in: String, expectedClass: ResponseClass): Unit = {
      val ctx = new DeserializeCtx(Echo.Echo.Args(in), deserializer)
      Contexts.local.let(DeserializeCtx.Key, ctx) {
        val rep = in.getBytes(Charsets.Utf8)
        assert(expectedClass == classifier(ReqRep(in, Return(rep))))
      }
    }

    def testApplyOrElse(in: String, expectedClass: ResponseClass): Unit = {
      val ctx = new DeserializeCtx(Echo.Echo.Args(in), deserializer)
      Contexts.local.let(DeserializeCtx.Key, ctx) {
        val rep = in.getBytes(Charsets.Utf8)
        assert(!classifier.isDefinedAt(ReqRep(in, Return(rep))))
        assert(expectedClass ==
          classifier.applyOrElse(
            ReqRep(in, Return(rep)),
            ResponseClassifier.Default))
      }
    }

    testApply("fail", NonRetryableFailure)
    testApplyOrElse("yep", Success)
  }

  test("DeserializeCtxOnly only deserializes and sees Thrift exceptions as success") {
    val in = "fail"
    val ctx = new DeserializeCtx(Echo.Echo.Args(in), deserializer)
    Contexts.local.let(DeserializeCtx.Key, ctx) {
      assert(deserializer(in.getBytes(Charsets.Utf8)).isThrow)
      val rep = in.getBytes(Charsets.Utf8)
      val reqRep = ReqRep(in, Return(rep))
      assert(ThriftResponseClassifier.DeserializeCtxOnly.isDefinedAt(reqRep))
      assert(Success == ThriftResponseClassifier.DeserializeCtxOnly(reqRep))
    }
  }


}
