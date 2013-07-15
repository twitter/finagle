package com.twitter.finagle.thrift

import com.twitter.finagle.{Service, WriteException}
import com.twitter.util.{Await, Future, Promise, Return, Throw}
import org.apache.thrift.TApplicationException
import org.apache.thrift.protocol.{TBinaryProtocol, TMessage, TMessageType}
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class ValidateThriftServiceSpec extends SpecificationWithJUnit with Mockito {
  val protocolFactory = new TBinaryProtocol.Factory()

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
      val buf = new OutputBuffer(protocolFactory)
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
        val buf = new OutputBuffer(protocolFactory)
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
        Await.result(f) must be_==(arr)
        validate.isAvailable must beFalse
        val resp = validate(req).poll

        resp.isDefined must beTrue
        resp.get.isThrow must beTrue

        val thrown = resp.get.asInstanceOf[Throw[Array[Byte]]].e
        thrown.isInstanceOf[WriteException] must beTrue
        thrown.getCause.isInstanceOf[InvalidThriftConnectionException] must beTrue
      }
    }

    "not invalidate connection on OK TApplicationException" in {
      val codes = Seq(TApplicationException.INTERNAL_ERROR,
        TApplicationException.UNKNOWN_METHOD)

      for (typ <- codes) {
        val buf = new OutputBuffer(protocolFactory)
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
        Await.result(f) must be_==(arr)
        validate.isAvailable must beTrue
        validate(req).poll must beLike {
          case Some(Return(_)) => true
        }
      }
    }
  }
}
