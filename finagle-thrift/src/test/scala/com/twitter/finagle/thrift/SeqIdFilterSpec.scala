package com.twitter.finagle.thrift

import com.twitter.finagle.Service
import com.twitter.util.{Return, Throw, Promise, Time}
import org.apache.thrift.protocol.{TBinaryProtocol, TMessage, TMessageType}
import org.apache.thrift.transport.TMemoryBuffer
import org.junit.runner.RunWith
import org.mockito.{Matchers, ArgumentCaptor}
import org.mockito.Mockito.{verify, when}
import org.scalatest.{OneInstancePerTest, FunSuite}
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class SeqIdFilterSpec extends FunSuite with MockitoSugar with OneInstancePerTest{
  val protocolFactory = new TBinaryProtocol.Factory()

  def mkmsg(tmsg: TMessage, strictWrite: Boolean) = {
    val trans = new TMemoryBuffer(24)
    val oprot = (new TBinaryProtocol.Factory(false, strictWrite)).getProtocol(trans)
    oprot.writeMessageBegin(tmsg)
    oprot.writeMessageEnd()
    trans.getArray()
  }

  def getmsg(buf: Array[Byte]) =
    new InputBuffer(buf, protocolFactory)().readMessageBegin

  for (seqId <- Seq(0, 1, -1, 123, -123, Int.MaxValue, Int.MinValue)) {
    testFilter("strict(%d)".format(seqId), seqId, mkmsg(_, true))
    testFilter("nonstrict(%s)".format(seqId), seqId, mkmsg(_, false))
  }

  def testFilter(how: String, seqId: Int, mkmsg: TMessage => Array[Byte]) {
    val service = mock[Service[ThriftClientRequest, Array[Byte]]]
    val p = new Promise[Array[Byte]]
    when(service(Matchers.any[ThriftClientRequest])).thenReturn(p)
    val filter = new SeqIdFilter
    val filtered = filter andThen service

    test("SeqIdFilter(%s)".format(how) + "maintain seqids passed in by the client") {
      val f = filtered(new ThriftClientRequest(mkmsg(new TMessage("proc", TMessageType.CALL, seqId)), false))
      assert(f.poll === None)

      val req = ArgumentCaptor.forClass(classOf[ThriftClientRequest])
      verify(service).apply(req.capture)
      p.setValue(mkmsg(new TMessage("proc", TMessageType.REPLY, getmsg(req.getValue.message).seqid)))

      assert(f.poll match {
        case Some(Return(buf)) => getmsg(buf).seqid == seqId
        case _ => false
      })
    }

    test("SeqIdFilter(%s)".format(how) + "use its own seqids to the server")  {Time.withCurrentTimeFrozen { _ =>
      val filtered = new SeqIdFilter andThen service
      val expected = (new scala.util.Random(Time.now.inMilliseconds)).nextInt()
      val f = filtered(new ThriftClientRequest(mkmsg(new TMessage("proc", TMessageType.CALL, seqId)), false))
      val req = ArgumentCaptor.forClass(classOf[ThriftClientRequest])
      verify(service).apply(req.capture)
      assert(getmsg(req.getValue.message).seqid === expected)
    }}

    test("SeqIdFilter(%s)".format(how) + "fail when sequence ids are out of order") { Time.withCurrentTimeFrozen { _ =>
      val filtered = new SeqIdFilter andThen service
      val expected = (new scala.util.Random(Time.now.inMilliseconds)).nextInt()
      val f = filtered(new ThriftClientRequest(mkmsg(new TMessage("proc", TMessageType.CALL, seqId)), false))
      p.setValue(mkmsg(new TMessage("proc", TMessageType.REPLY, 1111)))
      assert(f.poll match {
        case Some(Throw(SeqMismatchException(1111, expected))) => true
        case _ => false
      })
    }}

    def mustExcept(bytes: Array[Byte], exceptionMsg: String) {
      assert(filtered(new ThriftClientRequest(bytes, false)).poll match {
        case Some(Throw(exc: IllegalArgumentException)) => exc.getMessage == exceptionMsg
        case _ => false
      })
    }

    test("SeqIdFilter(%s)".format(how) + "must not modify the underlying request buffer") {
      val reqBuf = mkmsg(new TMessage("proc", TMessageType.CALL, 0))
      val origBuf = reqBuf.clone()
      filtered(new ThriftClientRequest(reqBuf, false))
      verify(service).apply(Matchers.any[ThriftClientRequest])
      assert(reqBuf.toSeq === origBuf.toSeq)
    }

    test("SeqIdFilter(%s)".format(how) + "handle empty TMessage") {
      mustExcept(Array(), "short header")
    }

    test("SeqIdFilter(%s)".format(how) + "handle short name size") {
      mustExcept(Array(-128, 1, 0, 0, 0, 0, 0), "short name size")
    }

    test("SeqIdFilter(%s)".format(how) + "handle old short buffer") {
      mustExcept(Array(0, 0, 0, 1, 0, 0, 0, 0, 1), "short buffer")
    }
  }
}
