package com.twitter.finagle.thrift.service

import com.twitter.finagle.context.Contexts
import com.twitter.finagle.service.{ReqRep, ResponseClass, ResponseClassifier}
import com.twitter.finagle.service.ResponseClass._
import com.twitter.finagle.thrift.{ClientDeserializeCtx, ServerToReqRep}
import com.twitter.finagle.thrift.thriftscala.{Echo, InvalidQueryException}
import com.twitter.util.{Return, Throw, Try}
import java.nio.charset.StandardCharsets.UTF_8
import org.scalatest.funsuite.AnyFunSuite

class ThriftResponseClassifierTest extends AnyFunSuite {

  private val classifier: ResponseClassifier = {
    case ReqRep(_, Return(rep: String)) if rep == "nope" => RetryableFailure
    case ReqRep(_, Throw(e: InvalidQueryException)) if e.errorCode == 4 => NonRetryableFailure
    case ReqRep(Echo.Echo.Args(in), _) if in == "lol" => NonRetryableFailure
  }

  private val clntClassifier =
    ThriftResponseClassifier.usingDeserializeCtx(classifier)
  private val srvClassifier =
    ThriftResponseClassifier.usingReqRepCtx(classifier)

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
      val clntCtx = new ClientDeserializeCtx(Echo.Echo.Args(in), deserializer)
      Contexts.local.let(ClientDeserializeCtx.Key, clntCtx) {
        val rep = in.getBytes(UTF_8)
        assert(expectedClass == clntClassifier(ReqRep(in, Return(rep))))
      }
    }
    testApply("nope", RetryableFailure)
    testApply("lol", NonRetryableFailure)
    testApply("fail", NonRetryableFailure)

    def testApplyOrElse(in: String, expectedClass: ResponseClass): Unit = {
      val clntCtx = new ClientDeserializeCtx(Echo.Echo.Args(in), deserializer)
      Contexts.local.let(ClientDeserializeCtx.Key, clntCtx) {
        val rep = in.getBytes(UTF_8)
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
        deserCtx.setReqRep(ReqRep(Echo.Echo.Args(in), getRep(in)))
        val rep = in.getBytes(UTF_8)
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
        val rep = in.getBytes(UTF_8)
        deserCtx.setReqRep(ReqRep(Echo.Echo.Args(in), getRep(in)))
        assert(!srvClassifier.isDefinedAt(ReqRep(Echo.Echo.Args(in), Return(rep))))
        assert(
          expectedClass ==
            srvClassifier.applyOrElse(ReqRep(in, Return(rep)), ResponseClassifier.Default)
        )
      }
    }
    testApplyOrElse("yep", Success)
    testApplyOrElse("failing", Success) // falls through to the default classifier
  }

  test("usingDeserializeCtx handles no DeserializationCtx") {
    def testApply(in: String, expectedClass: ResponseClass): Unit = {
      val rep = in.getBytes(UTF_8)
      assert(
        expectedClass ==
          clntClassifier.applyOrElse(ReqRep(in, Return(rep)), ResponseClassifier.Default)
      )

      assert(
        expectedClass ==
          srvClassifier.applyOrElse(ReqRep(in, Return(rep)), ResponseClassifier.Default)
      )
    }
    testApply("nope", Success)
    testApply("lol", Success)
    testApply("fail", Success)
  }

  test("ThriftExceptionsAsFailures") {
    import ThriftResponseClassifier.{ThriftExceptionsAsFailures, usingDeserializeCtx}

    val clntClassifier = usingDeserializeCtx(ThriftExceptionsAsFailures)
    assert("Thrift.usingDeserializeCtx(ThriftExceptionsAsFailures)" == clntClassifier.toString())

    def testApply(in: String, expectedClass: ResponseClass): Unit = {
      val ctx = new ClientDeserializeCtx(Echo.Echo.Args(in), deserializer)
      Contexts.local.let(ClientDeserializeCtx.Key, ctx) {
        val rep = in.getBytes(UTF_8)
        assert(expectedClass == clntClassifier(ReqRep(in, Return(rep))))
      }
    }

    def testApplyOrElse(in: String, expectedClass: ResponseClass): Unit = {
      val ctx = new ClientDeserializeCtx(Echo.Echo.Args(in), deserializer)
      Contexts.local.let(ClientDeserializeCtx.Key, ctx) {
        val rep = in.getBytes(UTF_8)
        assert(!clntClassifier.isDefinedAt(ReqRep(in, Return(rep))))
        assert(
          expectedClass ==
            clntClassifier.applyOrElse(ReqRep(in, Return(rep)), ResponseClassifier.Default)
        )
      }
    }

    testApply("fail", NonRetryableFailure)
    testApplyOrElse("yep", Success)
  }

  test("DeserializeCtxOnly only deserializes and sees Thrift exceptions as success") {
    val in = "fail"
    val clntCtx = new ClientDeserializeCtx(Echo.Echo.Args(in), deserializer)
    Contexts.local.let(ClientDeserializeCtx.Key, clntCtx) {
      assert(deserializer(in.getBytes(UTF_8)).isThrow)
      val rep = in.getBytes(UTF_8)
      val reqRep = ReqRep(in, Return(rep))
      assert(ThriftResponseClassifier.DeserializeCtxOnly.isDefinedAt(reqRep))
      assert(Success == ThriftResponseClassifier.DeserializeCtxOnly(reqRep))
    }
  }

  test("unset ServerToReqRep falls back to NoDeserializeCtx") {
    val srvCtx = new ServerToReqRep
    Contexts.local.let(ServerToReqRep.Key, srvCtx) {
      val req = "someString"
      val rep = req.getBytes(UTF_8)
      assert(!srvClassifier.isDefinedAt(ReqRep(req, Return(rep))))
      assert(
        Success == srvClassifier.applyOrElse(ReqRep(req, Return(rep)), ResponseClassifier.Default)
      )
    }
  }

}
