package com.twitter.finagle.thrift

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Status
import com.twitter.finagle.Service
import com.twitter.finagle.WriteException
import com.twitter.util.Await
import com.twitter.util.Future
import com.twitter.util.Promise
import com.twitter.util.Return
import com.twitter.util.Throw
import org.apache.thrift.TApplicationException
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.protocol.TMessage
import org.apache.thrift.protocol.TMessageType
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.mockito.Mockito.times
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class ValidateThriftServiceTest extends AnyFunSuite with MockitoSugar {

  case class ValidateThriftServiceContext(p: Promise[Array[Byte]] = new Promise[Array[Byte]]) {
    def newValidate() = new ValidateThriftService(service, protocolFactory)

    lazy val service: Service[ThriftClientRequest, Array[Byte]] = {
      val service = mock[Service[ThriftClientRequest, Array[Byte]]]
      when(service(ArgumentMatchers.any[ThriftClientRequest])).thenReturn(p)
      when(service.status).thenReturn(Status.Open)
      service
    }
    val req: ThriftClientRequest = mock[ThriftClientRequest]
    lazy val validate = newValidate()
    lazy val protocolFactory = new TBinaryProtocol.Factory()
  }

  test("ValidateThriftService should query availability from underlying") {
    val c = ValidateThriftServiceContext()
    import c._

    when(service.status).thenReturn(Status.Open)
    assert(validate.isAvailable)
    verify(service).status
    when(service.status).thenReturn(Status.Closed)
    assert(!validate.isAvailable)
    verify(service, times(2)).status
  }

  test("ValidateThriftService should handle no-exception messages") {
    val c = ValidateThriftServiceContext()
    import c._

    val buf = new OutputBuffer(protocolFactory)
    buf().writeMessageBegin(new TMessage("ok123", TMessageType.REPLY, 0))
    buf().writeMessageEnd()
    val res = validate(req)
    assert(res.isDefined == false)
    verify(service).apply(req)
    p.setValue(buf.toArray)
    assert(res.isDefined)
    assert(validate.isAvailable)
  }

  test("ValidateThriftService should invalidate connection on bad TApplicationException") {
    val c = ValidateThriftServiceContext()
    import c._

    val codes = Seq(
      TApplicationException.BAD_SEQUENCE_ID,
      TApplicationException.INVALID_MESSAGE_TYPE,
      TApplicationException.MISSING_RESULT,
      TApplicationException.UNKNOWN,
      TApplicationException.WRONG_METHOD_NAME
    )

    for (typ <- codes) {
      val buf = new OutputBuffer(protocolFactory)
      buf().writeMessageBegin(new TMessage("ok123", TMessageType.EXCEPTION, 0))
      val exc = new TApplicationException(typ, "wtf")
      exc.write(buf())
      buf().writeMessageEnd()
      val validate = newValidate()
      val arr = buf.toArray
      when(service(ArgumentMatchers.any[ThriftClientRequest])).thenReturn(Future.value(arr))
      assert(validate.isAvailable)
      val f = validate(req)
      assert(f.isDefined)
      assert(Await.result(f, 10.seconds) == arr)
      assert(!validate.isAvailable)
      val resp = validate(req).poll

      assert(resp.isDefined)
      assert(resp.get.isThrow)

      val thrown = resp.get.asInstanceOf[Throw[Array[Byte]]].e
      assert(thrown.isInstanceOf[WriteException])
      assert(thrown.getCause.isInstanceOf[InvalidThriftConnectionException])
    }
  }

  test("ValidateThriftService should not invalidate connection on OK TApplicationException") {
    val c = ValidateThriftServiceContext()
    import c._

    val codes = Seq(TApplicationException.INTERNAL_ERROR, TApplicationException.UNKNOWN_METHOD)

    for (typ <- codes) {
      val buf = new OutputBuffer(protocolFactory)
      buf().writeMessageBegin(new TMessage("foobar", TMessageType.EXCEPTION, 0))
      val exc = new TApplicationException(typ, "it's ok, don't worry about it!")
      exc.write(buf())
      buf().writeMessageEnd()
      val validate = newValidate()
      val arr = buf.toArray
      when(service(ArgumentMatchers.any[ThriftClientRequest])).thenReturn(Future.value(arr))
      assert(validate.isAvailable)
      val f = validate(req)
      assert(f.isDefined)
      assert(Await.result(f, 10.seconds) == arr)
      assert(validate.isAvailable)
      assert(validate(req).poll match {
        case Some(Return(_)) => true
        case _ => false
      })
    }
  }
}
