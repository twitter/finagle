package com.twitter.finagle.thrift.exp.partitioning

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.thrift.ThriftClientRequest
import com.twitter.finagle.thrift.exp.partitioning.PartitioningStrategy.{
  RequestMerger,
  ResponseMerger
}
import com.twitter.finagle.thrift.exp.partitioning.ThriftPartitioningService.ReqRepMarshallable
import com.twitter.io.Buf
import com.twitter.scrooge.ThriftStructIface
import com.twitter.test.thriftscala.B
import com.twitter.util.{Await, Awaitable, Duration, Return}
import org.scalatestplus.mockito.MockitoSugar

import org.apache.thrift.transport.TMemoryInputTransport
import com.twitter.finagle.thrift.Protocols

trait ThriftPartitioningTest extends MockitoSugar {

  def await[T](a: Awaitable[T], d: Duration = 5.seconds): T =
    Await.result(a, d)

  val AMethod = B.MergeableAdd
  val aResponseMerger: ResponseMerger[Int] = (success, _) => Return(success.sum)
  val aRequestMerger: RequestMerger[B.MergeableAdd.Args] =
    (aRequests: Seq[B.MergeableAdd.Args]) => B.MergeableAdd.Args(aRequests.flatten(_.alist))

  val thriftMarshallable: ReqRepMarshallable[ThriftStructIface, Int] =
    new ReqRepMarshallable[ThriftStructIface, Int] {
      def framePartitionedRequest(
        thriftClientRequest: ThriftClientRequest,
        original: ThriftStructIface
      ): ThriftStructIface = {
        val transport = new TMemoryInputTransport(thriftClientRequest.message)
        val protoFactory = Protocols.binaryFactory()
        val prot = protoFactory.getProtocol(transport)
        prot.readMessageBegin()
        B.MergeableAdd.Args.decode(prot)
      }

      def isOneway(original: ThriftStructIface): Boolean = false

      def fromResponseToBytes(rep: Int): Array[Byte] = {
        Buf.ByteArray.Owned.extract(Buf.U32BE(rep))
      }

      def emptyResponse: Int = 0
    }
}
