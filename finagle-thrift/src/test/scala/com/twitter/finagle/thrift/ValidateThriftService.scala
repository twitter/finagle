package com.twitter.finagle.thrift

import com.twitter.finagle.{Service, WriteException}
import org.apache.thrift.TApplicationException
import org.apache.thrift.protocol.{TBinaryProtocol, TMessageType, TMessage}
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import com.twitter.util.{Future, Promise, Throw, Return}

class ValidateThriftServiceSpec extends SpecificationWithJUnit with Mockito {
  "ValidateThriftService" should {
    val p = new Promise[Array[Byte]]
    val service = mock[Service[ThriftClientRequest, Array[Byte]]]
    service(any) returns p
    service.isAvailable returns true
    def newValidate() = new ValidateThriftService(service, new TBinaryProtocol.Factory())
    val validate = newValidate()
    val req = mock[ThriftClientRequest]

    "query availability from underlying" in {
      service.isAvailable returns true
      validate.isAvailable must beTrue
      there was one(service).isAvailable
      service.isAvailable returns false
      validate.isAvailable must beFalse
      there were two(service).isAvailable
    }

    "handle no-exception messages" in {
      val buf = new OutputBuffer()
      buf().writeMessageBegin(new TMessage("ok123", TMessageType.REPLY, 0))
      buf().writeMessageEnd()
      val res = validate(req)
      res.isDefined must beFalse
      there was one(service).apply(req)
      p.setValue(buf.toArray)
      res.isDefined must beTrue
      validate.isAvailable must beTrue
    }

    "invalidate connection on bad TApplicationException" in {
      val codes = Seq(
        TApplicationException.BAD_SEQUENCE_ID,
        TApplicationException.INVALID_MESSAGE_TYPE,
        TApplicationException.MISSING_RESULT,
        TApplicationException.UNKNOWN,
        TApplicationException.WRONG_METHOD_NAME)

      for (typ <- codes) {
        val buf = new OutputBuffer()
        buf().writeMessageBegin(new TMessage("ok123", TMessageType.EXCEPTION, 0))
        val exc = new TApplicationException(typ, "wtf")
        exc.write(buf())
        buf().writeMessageEnd()
        val validate = newValidate()
        val arr = buf.toArray
        service(any) returns Future.value(arr)
        validate.isAvailable must beTrue
        val f = validate(req)
        f.isDefined must beTrue
        f() must be_==(arr)
        validate.isAvailable must beFalse
        validate(req).poll must beSome(
          Throw(WriteException(InvalidThriftConnectionException())))
      }
    }

    "not invalidate connection on OK TApplicationException" in {
      val codes = Seq(TApplicationException.INTERNAL_ERROR,
        TApplicationException.UNKNOWN_METHOD)

      for (typ <- codes) {
        val buf = new OutputBuffer()
        buf().writeMessageBegin(new TMessage("foobar", TMessageType.EXCEPTION, 0))
        val exc = new TApplicationException(typ, "it's ok, don't worry about it!")
        exc.write(buf())
        buf().writeMessageEnd()
        val validate = newValidate()
        val arr = buf.toArray
        service(any) returns Future.value(arr)
        validate.isAvailable must beTrue
        val f = validate(req)
        f.isDefined must beTrue
        f() must be_==(arr)
        validate.isAvailable must beTrue
        validate(req).poll must beLike {
          case Some(Return(_)) => true
        }
      }
    }
  }
}
