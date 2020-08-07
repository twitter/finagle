package com.twitter.finagle.thrift.exp.partitioning

import com.twitter.finagle.thrift.exp.partitioning.PartitioningStrategy.{
  RequestMerger,
  RequestMergerRegistry,
  ResponseMerger,
  ResponseMergerRegistry
}
import com.twitter.scrooge.{ThriftMethodIface, ThriftStructIface}
import com.twitter.util.{Return, Throw}
import org.apache.thrift.protocol.TProtocol
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatestplus.mockito.MockitoSugar

class PartitioningStrategyTest extends FunSuite with MockitoSugar {
  val AMethod = mock[ThriftMethodIface]
  when(AMethod.name).thenReturn("A")

  val BMethod = mock[ThriftMethodIface]
  when(BMethod.name).thenReturn("B")

  val CMethod = mock[ThriftMethodIface]
  when(CMethod.name).thenReturn("C")

  case class ARequest(a: Int) extends ThriftStructIface {
    def write(oprot: TProtocol): Unit = ()
  }
  case class BRequest(b: String) extends ThriftStructIface {
    def write(oprot: TProtocol): Unit = ()
  }
  case class CRequest(c: Int) extends ThriftStructIface {
    def write(oprot: TProtocol): Unit = ()
  }

  test("RequestMergerRegistry RequestMerger handles multiple endpoints") {
    val requestMergerRegistry = new RequestMergerRegistry
    val aMerger: RequestMerger[ARequest] = as => as.head
    val bMerger: RequestMerger[BRequest] = bs => bs.last
    requestMergerRegistry.add(AMethod, aMerger).add(BMethod, bMerger)

    val getAMerger = requestMergerRegistry.get(AMethod.name).get
    assert(getAMerger(Seq(ARequest(1), ARequest(2))) == ARequest(1))

    val getBMerger = requestMergerRegistry.get(BMethod.name).get
    assert(getBMerger(Seq(BRequest("1"), BRequest("2"))) == BRequest("2"))
  }

  test("ResponseMergerRegistry ResponseMerger handles multiple endpoints") {
    val responseMergerRegistry = new ResponseMergerRegistry
    val aMerger: ResponseMerger[Int] = (success, _) => Return(success.head)
    val bMerger: ResponseMerger[String] = (_, failures) => Throw(failures.last)
    responseMergerRegistry.add(AMethod, aMerger).add(BMethod, bMerger)

    val getAMerger = responseMergerRegistry.get(AMethod.name).get
    assert(getAMerger(Seq(1, 2, 3), Seq.empty) == Return(1))

    val getBMerger = responseMergerRegistry.get(BMethod.name).get
    assert(
      getBMerger(
        Seq.empty,
        Seq(new Exception("1"), new Exception("2"))).throwable.getMessage == "2")
  }

  test("unset endpoints behavior has default None to original request") {
    val hashingStrategy = new ClientHashingStrategy({
      case a: ARequest => Map(1 -> a)
      case b: BRequest => Map("some hashing key" -> b)
    })
    val result = hashingStrategy.getHashingKeyAndRequest
      .applyOrElse(CRequest(1), ClientHashingStrategy.defaultHashingKeyAndRequest)
    assert(result == Map(None -> CRequest(1)))
  }
}
