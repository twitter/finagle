package com.twitter.finagle.benchmark

import com.google.caliper.SimpleBenchmark
import com.twitter.finagle.Service
import com.twitter.finagle.benchmark.thrift.Hello
import com.twitter.finagle.exp.swift._
import com.twitter.finagle.thrift.ThriftClientRequest
import com.twitter.util.{Await, Future}
import java.util.Arrays
import org.apache.thrift.protocol._
import org.apache.thrift.transport._

@ThriftService
trait SwiftHello {
  @ThriftMethod def echo(body: String): Future[String]
}

class ThriftDispatchBenchmark extends SimpleBenchmark {
  val scroogeService = {
    val impl = new Hello.FutureIface {
      def echo(body: String) = Future.value(body)
    }
    new Hello.FinagledService(impl, new TBinaryProtocol.Factory())
  }

  val swiftService = {
    val impl = new SwiftHello {
      def echo(body: String) = Future.value(body)
    }
    new SwiftService(impl)
  }

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

  val scroogeClient = new Hello.FinagledClient(service)
  val swiftClient = SwiftProxy.newClient[SwiftHello](service)

  def timeScroogeDispatch(nreps: Int) {
    var i = 0
    while (i < nreps) {
      Await.ready(scroogeService(payload))
      i += 1
    }
  }
  
  def timeSwiftDispatch(nreps: Int) {
    var i = 0
    while (i < nreps) {
      Await.ready(swiftService(payload))
      i += 1
    }
  }
  
  def timeScroogeProxy(nreps: Int) {
    var i = 0
    while (i < nreps) {
      Await.ready(scroogeClient.echo("hello world"))
      i += 1
    }
  }

  def timeSwiftProxy(nreps: Int) {
    var i = 0
    while (i < nreps) {
      Await.ready(swiftClient.echo("hello world"))
      i += 1
    }
  }
}
