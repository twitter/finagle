package com.twitter.finagle.thriftmux.service

import com.twitter.finagle.context.Contexts
import com.twitter.finagle.mux
import com.twitter.finagle.service.{ReqRep, ResponseClass, ResponseClassifier}
import com.twitter.finagle.service.ResponseClass._
import com.twitter.finagle.thrift.{ClientDeserializeCtx, ServerToReqRep}
import com.twitter.finagle.thriftmux.thriftscala.{InvalidQueryException, TestService}
import com.twitter.io.Buf
import com.twitter.util.{Return, Throw, Try}
import java.nio.charset.StandardCharsets.UTF_8
import org.scalatest.funsuite.AnyFunSuite

class ThriftMuxResponseClassifierTest extends AnyFunSuite {

  private val classifier: ResponseClassifier = {
    case ReqRep(_, Return(rep: String)) if rep == "nope" => RetryableFailure
    case ReqRep(_, Throw(e: InvalidQueryException)) if e.errorCode == 4 => NonRetryableFailure
    case ReqRep(TestService.Query.Args(in), _) if in == "lol" => NonRetryableFailure
  }

  private val clntClassifier =
    ThriftMuxResponseClassifier.usingDeserializeCtx(classifier)

  private val srvClassifier =
    ThriftMuxResponseClassifier.usingReqRepCtx(classifier)

  private val deserializer = { bytes: Array[Byte] =>
    val asString = new String(bytes, UTF_8)
    if (asString.startsWith("fail")) Throw(new InvalidQueryException(asString.length))
    else Return(asString)
  }

  private def getRep(in: String): Try[_] =
    if (in.startsWith("fail")) {
      Throw(new InvalidQueryException(in.length))
    } else Return(in)

  test("usingDeserializeCtx basics for ClientDeserializeCtx") {
    def testApply(in: String, expectedClass: ResponseClass): Unit = {
      val clntCtx = new ClientDeserializeCtx(TestService.Query.Args(in), deserializer)
      Contexts.local.let(ClientDeserializeCtx.Key, clntCtx) {
        val rep = mux.Response(Nil, Buf.Utf8(in))
        assert(expectedClass == clntClassifier(ReqRep(in, Return(rep))))
      }
    }
    testApply("nope", RetryableFailure)
    testApply("lol", NonRetryableFailure)
    testApply("fail", NonRetryableFailure)

    def testApplyOrElse(in: String, expectedClass: ResponseClass): Unit = {
      val clntCtx = new ClientDeserializeCtx(TestService.Query.Args(in), deserializer)
      Contexts.local.let(ClientDeserializeCtx.Key, clntCtx) {
        val rep = mux.Response(Nil, Buf.Utf8(in))
        assert(!clntClassifier.isDefinedAt(ReqRep(in, Return(rep))))
        assert(
          expectedClass ==
            clntClassifier.applyOrElse(ReqRep(in, Return(rep)), ResponseClassifier.Default)
        )
      }
    }
    testApplyOrElse("yep", Success)
    testApplyOrElse("failing", Success) // falls through to the default classifier
  }

  test("usingDeserializeCtx basics for ServerToReqRep") {
    def testApply(in: String, expectedClass: ResponseClass): Unit = {
      val srvCtx = new ServerToReqRep
      Contexts.local.let(ServerToReqRep.Key, srvCtx) {
        val deserCtx = Contexts.local.get(ServerToReqRep.Key).get
        deserCtx.setReqRep(ReqRep(TestService.Query.Args(in), getRep(in)))
        val rep = mux.Response(Nil, Buf.Utf8(in))
        assert(expectedClass == srvClassifier(ReqRep(in, Return(rep))))
      }
    }
    testApply("nope", RetryableFailure)
    testApply("lol", NonRetryableFailure)
    testApply("fail", NonRetryableFailure)

    def testApplyOrElse(in: String, expectedClass: ResponseClass): Unit = {
      val srvCtx = new ServerToReqRep
      Contexts.local.let(ServerToReqRep.Key, srvCtx) {
        val deserCtx = Contexts.local.get(ServerToReqRep.Key).get
        deserCtx.setReqRep(ReqRep(TestService.Query.Args(in), getRep(in)))
        val rep = mux.Response(Nil, Buf.Utf8(in))
        assert(!srvClassifier.isDefinedAt(ReqRep(in, Return(rep))))
        assert(
          expectedClass ==
            srvClassifier.applyOrElse(ReqRep(in, Return(rep)), ResponseClassifier.Default)
        )
      }
    }
    testApplyOrElse("yep", Success)
    testApplyOrElse("failing", Success) // falls through to the default classifier
  }

  test("usingDeserializeCtx ignores exceptions during deserialization") {
    val toThrow = new RuntimeException("welp")
    val throwingDeser = { bytes: Array[Byte] => throw toThrow }

    val input = "throw"
    val ctx = new ClientDeserializeCtx(TestService.Query.Args(input), throwingDeser)
    Contexts.local.let(ClientDeserializeCtx.Key, ctx) {
      val rep = mux.Response(Nil, Buf.Utf8(input))

      assert(!classifier.isDefinedAt(ReqRep(input, Return(rep))))
      assert(
        Success ==
          classifier.applyOrElse(ReqRep(input, Return(rep)), ResponseClassifier.Default)
      )
    }
  }

  test("usingDeserializeCtx handles no DeserializationCtx") {
    def testApply(in: String, expectedClass: ResponseClass): Unit = {
      val rep = mux.Response(Nil, Buf.Utf8(in))
      assert(
        expectedClass ==
          classifier.applyOrElse(ReqRep(in, Return(rep)), ResponseClassifier.Default)
      )
    }
    testApply("nope", Success)
    testApply("lol", Success)
    testApply("fail", Success)
  }

  test("ThriftExceptionsAsFailures") {
    import ThriftMuxResponseClassifier.{ThriftExceptionsAsFailures, usingDeserializeCtx}

    val classifier = usingDeserializeCtx(ThriftExceptionsAsFailures)
    assert("ThriftMux.usingDeserializeCtx(ThriftExceptionsAsFailures)" == classifier.toString())

    def testApply(in: String, expectedClass: ResponseClass): Unit = {
      val ctx = new ClientDeserializeCtx(TestService.Query.Args(in), deserializer)
      Contexts.local.let(ClientDeserializeCtx.Key, ctx) {
        val rep = mux.Response(Nil, Buf.Utf8(in))
        assert(expectedClass == classifier(ReqRep(in, Return(rep))))
      }
    }

    def testApplyOrElse(in: String, expectedClass: ResponseClass): Unit = {
      val ctx = new ClientDeserializeCtx(TestService.Query.Args(in), deserializer)
      Contexts.local.let(ClientDeserializeCtx.Key, ctx) {
        val rep = mux.Response(Nil, Buf.Utf8(in))
        assert(!classifier.isDefinedAt(ReqRep(in, Return(rep))))
        assert(
          expectedClass ==
            classifier.applyOrElse(ReqRep(in, Return(rep)), ResponseClassifier.Default)
        )
      }
    }

    testApply("fail", NonRetryableFailure)
    testApplyOrElse("yep", Success)
  }

  test("DeserializeCtxOnly only deserializes and sees Thrift exceptions as success") {
    val in = "fail"
    val ctx = new ClientDeserializeCtx(TestService.Query.Args(in), deserializer)
    Contexts.local.let(ClientDeserializeCtx.Key, ctx) {
      assert(deserializer(in.getBytes(UTF_8)).isThrow)
      val rep = mux.Response(Nil, Buf.Utf8(in))
      val reqRep = ReqRep(in, Return(rep))
      assert(ThriftMuxResponseClassifier.DeserializeCtxOnly.isDefinedAt(reqRep))
      assert(Success == ThriftMuxResponseClassifier.DeserializeCtxOnly(reqRep))
    }
  }

}
