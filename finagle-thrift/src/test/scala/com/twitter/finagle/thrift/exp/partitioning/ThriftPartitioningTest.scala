package com.twitter.finagle.thrift.exp.partitioning

import com.twitter.finagle.thrift.{Protocols, ThriftClientRequest}
import com.twitter.finagle.thrift.exp.partitioning.PartitioningStrategy.{
  RequestMerger,
  ResponseMerger
}
import com.twitter.finagle.thrift.exp.partitioning.ThriftPartitioningService.ReqRepMarshallable
import com.twitter.io.Buf
import com.twitter.scrooge.{ThriftMethodIface, ThriftStruct}
import com.twitter.util.Return
import org.apache.thrift.protocol.{TList, TProtocol, TType}
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar

trait ThriftPartitioningTest extends MockitoSugar {
  case class ARequest(alist: Seq[Int], serialized: Option[Array[Byte]] = None)
      extends ThriftStruct {
    def write(oprot: TProtocol): Unit = {
      oprot.writeListBegin(new TList(TType.I32, alist.size))
      alist.foreach { elem => oprot.writeI32(elem) }
      oprot.writeListEnd()
    }
  }

  val AMethod = mock[ThriftMethodIface]
  when(AMethod.name).thenReturn("A")
  val aResponseMerger: ResponseMerger[Int] = (success, _) => Return(success.head)
  val aRequestMerger: RequestMerger[ARequest] = (aRequests: Seq[ARequest]) =>
    ARequest(aRequests.flatten(_.alist))

  val thriftMarshallable = new ReqRepMarshallable[ARequest, Int] {
    def framePartitionedRequest(rawRequest: ThriftClientRequest, original: ARequest): ARequest =
      ARequest(original.alist, Some(rawRequest.message))
    def isOneway(original: ARequest): Boolean = false
    def fromResponseToBytes(rep: Int): Array[Byte] = Buf.ByteArray.Owned.extract(Buf.U32BE(rep))
    val emptyResponse: Int = Int.MinValue
  }

  def deserialize(resBytes: Array[Byte]): Seq[Int] = {
    val iprot = Protocols
      .binaryFactory().getProtocol(new org.apache.thrift.transport.TMemoryInputTransport(resBytes))
    iprot.readMessageBegin()
    val tlist = iprot.readListBegin()
    val result: Seq[Int] = (0 until tlist.size).map { _ => iprot.readI32() }
    iprot.readListEnd()
    iprot.readMessageEnd()
    result
  }
}
