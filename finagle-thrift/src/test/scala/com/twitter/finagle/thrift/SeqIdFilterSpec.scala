package com.twitter.finagle.thrift

import com.twitter.finagle.Service
import com.twitter.util.{Return, Throw, Promise, Time}
import org.apache.thrift.protocol.{TBinaryProtocol, TMessage, TMessageType}
import org.apache.thrift.transport.TMemoryBuffer
import org.mockito.ArgumentCaptor
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class SeqIdFilterSpec extends SpecificationWithJUnit with Mockito {
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
    "SeqIdFilter(%s)".format(how) should {
      val service = mock[Service[ThriftClientRequest, Array[Byte]]]
      val p = new Promise[Array[Byte]]
      service(any) returns p
      val filter = new SeqIdFilter
      val filtered = filter andThen service

      "maintain seqids passed in by the client" in {
        val f = filtered(new ThriftClientRequest(mkmsg(new TMessage("proc", TMessageType.CALL, seqId)), false))
        f.poll must beNone

        val req = ArgumentCaptor.forClass(classOf[ThriftClientRequest])
        there was one(service).apply(req.capture)
        p.setValue(mkmsg(new TMessage("proc", TMessageType.REPLY, getmsg(req.getValue.message).seqid)))

        f.poll must beLike {
          case Some(Return(buf)) => getmsg(buf).seqid == seqId
        }
      }

      "use its own seqids to the server" in Time.withCurrentTimeFrozen { _ =>
        val filtered = new SeqIdFilter andThen service
        val expected = (new scala.util.Random(Time.now.inMilliseconds)).nextInt()
        val f = filtered(new ThriftClientRequest(mkmsg(new TMessage("proc", TMessageType.CALL, seqId)), false))
        val req = ArgumentCaptor.forClass(classOf[ThriftClientRequest])
        there was one(service).apply(req.capture)
        getmsg(req.getValue.message).seqid must be_==(expected)
      }

      "fail when sequence ids are out of order" in Time.withCurrentTimeFrozen { _ =>
        val filtered = new SeqIdFilter andThen service
        val expected = (new scala.util.Random(Time.now.inMilliseconds)).nextInt()
        val f = filtered(new ThriftClientRequest(mkmsg(new TMessage("proc", TMessageType.CALL, seqId)), false))
        p.setValue(mkmsg(new TMessage("proc", TMessageType.REPLY, 1111)))
        f.poll must beLike {
          case Some(Throw(SeqMismatchException(1111, expected))) => true
        }
      }

      def mustExcept(bytes: Array[Byte], exceptionMsg: String) {
        filtered(new ThriftClientRequest(bytes, false)).poll must beLike {
          case Some(Throw(exc: IllegalArgumentException)) => exc.getMessage == exceptionMsg
        }
      }

      "must not modify the underlying request buffer" in {
        val reqBuf = mkmsg(new TMessage("proc", TMessageType.CALL, 0))
        val origBuf = reqBuf.clone()
        filtered(new ThriftClientRequest(reqBuf, false))
        there was one(service).apply(any)
        reqBuf.toSeq must be_==(origBuf.toSeq)
      }

      "handle empty TMessage" in {
        mustExcept(Array(), "short header")
      }

      "handle short name size" in {
        mustExcept(Array(-128, 1, 0, 0, 0, 0, 0), "short name size")
      }

      "handle old short buffer" in {
        mustExcept(Array(0, 0, 0, 1, 0, 0, 0, 0, 1), "short buffer")
      }
    }
  }
}
