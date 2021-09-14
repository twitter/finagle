package com.twitter.finagle.thrift

import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.benchmark.thriftscala.Hello
import com.twitter.finagle.Service
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.util.{Await, Future}
import java.util.Arrays
import org.apache.thrift.protocol._
import org.apache.thrift.transport._
import org.openjdk.jmh.annotations._

object ThriftDispatchBench {
  val payload = {
    val buf = new TMemoryBuffer(32)
    val out = new TBinaryProtocol(buf)
    out.writeMessageBegin(new TMessage("echo", TMessageType.CALL, 0))
    out.writeStructBegin(new TStruct("echo_args"))
    out.writeFieldBegin(new TField("body", TType.STRING, 1))
    out.writeString("hello world")
    out.writeFieldEnd()
    out.writeFieldStop()
    out.writeStructEnd()
    out.writeMessageEnd()
    Arrays.copyOfRange(buf.getArray(), 0, buf.length())
  }

  val service = new Service[ThriftClientRequest, Array[Byte]] {
    val response = Future.value {
      val buf = new TMemoryBuffer(32)
      val out = new TBinaryProtocol(buf)
      out.writeMessageBegin(new TMessage("echo", TMessageType.REPLY, 0))
      out.writeStructBegin(new TStruct("echo_result"))
      out.writeFieldBegin(new TField("success", TType.STRING, 0))
      out.writeString("the response")
      out.writeFieldEnd()
      out.writeFieldStop()
      out.writeStructEnd()
      out.writeMessageEnd()
      Arrays.copyOfRange(buf.getArray(), 0, buf.length())
    }

    def apply(req: ThriftClientRequest) = response
  }

  def scroogeService(prot: TProtocolFactory) = {
    val impl = new Hello.MethodPerEndpoint {
      def echo(body: String) = Future.value(body)
    }
    new Hello.FinagledService(impl, prot)
  }

  def scroogeClient(prot: TProtocolFactory) = new Hello.FinagledClient(service, prot)
}

@Threads(1)
@State(Scope.Benchmark)
class ThriftDispatchBench extends StdBenchAnnotations {
  import ThriftDispatchBench._

  // standard TBinaryProtocol
  val scroogeService0 = scroogeService(new TBinaryProtocol.Factory())
  var scroogeClient0 = scroogeClient(new TBinaryProtocol.Factory())

  // Protocols.binaryFactory()
  val scroogeService1 = scroogeService(Protocols.binaryFactory(statsReceiver = NullStatsReceiver))
  val scroogeClient1 = scroogeClient(Protocols.binaryFactory(statsReceiver = NullStatsReceiver))

  @Benchmark
  def scroogeDispatch_StdTBinaryProt(): Array[Byte] = {
    Await.result(scroogeService0(payload))
  }

  @Benchmark
  def scroogeProxy_StdBinaryProt(): String = {
    Await.result(scroogeClient0.echo("hello world"))
  }

  @Benchmark
  def scroogeDispatch_CustomTBinaryProt(): Array[Byte] = {
    Await.result(scroogeService1(payload))
  }

  @Benchmark
  def scroogeProxy_CustomTBinaryProt(): String = {
    Await.result(scroogeClient1.echo("hello world"))
  }
}
