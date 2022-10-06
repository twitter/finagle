package com.twitter.finagle.thrift

import com.twitter.finagle.Service
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Promise
import com.twitter.util.Time
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.protocol.TMessage
import org.apache.thrift.protocol.TMessageType
import org.apache.thrift.transport.TMemoryBuffer
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.scalatest.OneInstancePerTest
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class SeqIdFilterTest extends AnyFunSuite with MockitoSugar with OneInstancePerTest {
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

  def testFilter(how: String, seqId: Int, mkmsg: TMessage => Array[Byte]): Unit = {
    val service = mock[Service[ThriftClientRequest, Array[Byte]]]
    val p = new Promise[Array[Byte]]
    when(service(ArgumentMatchers.any[ThriftClientRequest])).thenReturn(p)
    val filter = new SeqIdFilter
    val filtered = filter andThen service

    test("SeqIdFilter(%s) maintain seqids passed in by the client".format(how)) {
      val f = filtered(
        new ThriftClientRequest(mkmsg(new TMessage("proc", TMessageType.CALL, seqId)), false)
      )
      assert(f.poll == None)

      val req = ArgumentCaptor.forClass(classOf[ThriftClientRequest])
      verify(service).apply(req.capture)
      p.setValue(
        mkmsg(new TMessage("proc", TMessageType.REPLY, getmsg(req.getValue.message).seqid))
      )

      f.poll match {
        case Some(Return(buf)) => assert(getmsg(buf).seqid == seqId)
        case _ => fail()
      }
    }

    test("SeqIdFilter(%s) use its own seqids to the server".format(how)) {
      Time.withCurrentTimeFrozen { _ =>
        val filtered = new SeqIdFilter andThen service
        val expected = (new scala.util.Random(Time.now.inMilliseconds)).nextInt()
        val f = filtered(
          new ThriftClientRequest(mkmsg(new TMessage("proc", TMessageType.CALL, seqId)), false)
        )
        val req = ArgumentCaptor.forClass(classOf[ThriftClientRequest])
        verify(service).apply(req.capture)
        assert(getmsg(req.getValue.message).seqid == expected)
      }
    }

    test("SeqIdFilter(%s) fail when sequence ids are out of order".format(how)) {
      Time.withCurrentTimeFrozen { _ =>
        val filtered = new SeqIdFilter andThen service
        val expected = (new scala.util.Random(Time.now.inMilliseconds)).nextInt()
        val f = filtered(
          new ThriftClientRequest(mkmsg(new TMessage("proc", TMessageType.CALL, seqId)), false)
        )
        p.setValue(mkmsg(new TMessage("proc", TMessageType.REPLY, 1111)))
        assert(f.poll match {
          case Some(Throw(SeqMismatchException(1111, expected))) => true
          case _ => false
        })
      }
    }

    def mustExcept(bytes: Array[Byte], exceptionMsg: String): Unit = {
      filtered(new ThriftClientRequest(bytes, false)).poll match {
        case Some(Throw(exc: IllegalArgumentException)) => assert(exc.getMessage == exceptionMsg)
        case _ => fail()
      }
    }

    test("SeqIdFilter(%s) must not modify the underlying request buffer".format(how)) {
      val reqBuf = mkmsg(new TMessage("proc", TMessageType.CALL, 0))
      val origBuf = reqBuf.clone()
      filtered(new ThriftClientRequest(reqBuf, false))

      verify(service).apply(ArgumentMatchers.any[ThriftClientRequest])
      assert(reqBuf.toSeq == origBuf.toSeq)
    }

    test("SeqIdFilter(%s) handle empty TMessage".format(how)) {
      mustExcept(Array(), "short header")
    }

    test("SeqIdFilter(%s) handle short name size".format(how)) {
      mustExcept(Array(-128, 1, 0, 0, 0, 0, 0), "short name size")
    }

    test("SeqIdFilter(%s) handle old short buffer".format(how)) {
      mustExcept(Array(0, 0, 0, 1, 0, 0, 0, 0, 1), "short buffer")
    }
  }
}
