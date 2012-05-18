package com.twitter.finagle.thrift

import com.twitter.finagle.Service
import com.twitter.util.{Future, Return, Throw, Promise, Time}
import org.apache.thrift.protocol.{TBinaryProtocol, TMessage, TMessageType}
import org.apache.thrift.transport.TMemoryBuffer
import org.mockito.ArgumentCaptor
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class SeqIdFilterSpec extends SpecificationWithJUnit with Mockito {
  def mkmsg(tmsg: TMessage) = {
    val trans = new TMemoryBuffer(24)
    val oprot = (new TBinaryProtocol.Factory).getProtocol(trans)
    oprot.writeMessageBegin(tmsg)
    oprot.writeMessageEnd()
    trans.getArray()
  }

  def getmsg(buf: Array[Byte]) =
    new InputBuffer(buf)().readMessageBegin

  "SeqIdFilter" should {
    val service = mock[Service[ThriftClientRequest, Array[Byte]]]
    val p = new Promise[Array[Byte]]
    service(any) returns p
    val filter = new SeqIdFilter
    val filtered = filter andThen service

    "maintain seqids passed in by the client" in {
      val f = filtered(new ThriftClientRequest(mkmsg(new TMessage("proc", TMessageType.CALL, 123)), false))
      f.poll must beNone

      val req = ArgumentCaptor.forClass(classOf[ThriftClientRequest])
      there was one(service).apply(req.capture)
      p.setValue(mkmsg(new TMessage("proc", TMessageType.REPLY, getmsg(req.getValue.message).seqid)))

      f.poll must beLike {
        case Some(Return(buf)) => getmsg(buf).seqid == 123
      }
    }

    "use its own seqids to the server" in Time.withCurrentTimeFrozen { _ =>
      val filtered = new SeqIdFilter andThen service
      val expected = (new scala.util.Random(Time.now.inMilliseconds)).nextInt()
      val f = filtered(new ThriftClientRequest(mkmsg(new TMessage("proc", TMessageType.CALL, 123)), false))
      val req = ArgumentCaptor.forClass(classOf[ThriftClientRequest])
      there was one(service).apply(req.capture)
      getmsg(req.getValue.message).seqid must be_==(expected)
    }

    "fail when sequence ids are out of order" in Time.withCurrentTimeFrozen { _ =>
      val filtered = new SeqIdFilter andThen service
      val expected = (new scala.util.Random(Time.now.inMilliseconds)).nextInt()
      val f = filtered(new ThriftClientRequest(mkmsg(new TMessage("proc", TMessageType.CALL, 123)), false))
      p.setValue(mkmsg(new TMessage("proc", TMessageType.REPLY, 1111)))
      f.poll must beLike {
        case Some(Throw(SeqMismatchException(1111, expected))) => true
      }
    }
    
    "handle empty TMessage" in {
      filtered(new ThriftClientRequest(Array(), false)).poll must beLike {
        case Some(Throw(exc: IllegalArgumentException)) => exc.getMessage == "bad TMessage"
      }
    }
    
    "handle short TMessage" in {
      filtered(new ThriftClientRequest(Array(-1, 0, 0, 0, 0, 0, 0, 127), false)).poll must beLike {
        case Some(Throw(exc: IllegalArgumentException)) => exc.getMessage == "bad TMessage"
      }
    }
  }
}
