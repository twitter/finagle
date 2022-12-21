package com.twitter.finagle.thrift

import com.twitter.finagle.Service
import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.benchmark.thriftjava.{Request => TJavaRequest}
import com.twitter.finagle.benchmark.thriftjava.{Response => TJavaResponse}
import com.twitter.finagle.benchmark.thriftjava.{ThriftOneGenServer => TJavaThriftOneGenServer}
import com.twitter.finagle.benchmark.thriftscala.{Request => TScalaRequest}
import com.twitter.finagle.benchmark.thriftscala.{Response => TScalaResponse}
import com.twitter.finagle.benchmark.thriftscala.{ThriftOneGenServer => TScalaThriftOneGenServer}
import com.twitter.util.Await
import com.twitter.util.Future
import org.apache.thrift.protocol._
import org.openjdk.jmh.annotations._
import scala.collection.JavaConverters._

// bazel run //finagle/finagle-benchmark/src/main/scala:jmh -- 'ThriftGenBenchmark'
// bazel run //finagle/finagle-benchmark/src/main/scala:jmh -- 'ThriftGenBenchmark' -prof gc
@Threads(1)
@State(Scope.Benchmark)
class ThriftGenBenchmark extends StdBenchAnnotations {
  // as of 2022-09-09, the collection size for timelines services varies from 10s to low 1000s
  // here I assume 5000 is a reasonable upper bound for most services as of 2022-09-09
  @Param(Array("0", "10", "100", "1000", "5000"))
  var collectionSize: Int = _

  var javaRequest: TJavaRequest = _
  var scalaRequest: TScalaRequest = _
  var thriftRequestBytes: Array[Byte] = _
  var javaServer: Service[Array[Byte], Array[Byte]] = _
  var scalaServer: Service[Array[Byte], Array[Byte]] = _
  var javaClient: TJavaThriftOneGenServer.ServiceToClient = _
  var scalaClient: TScalaThriftOneGenServer.MethodPerEndpoint = _

  @Setup
  def setup() = {
    javaRequest = new TJavaRequest(
      Int.MaxValue,
      Long.MaxValue,
      false,
      "hello",
      Seq.fill(collectionSize)("hello").asJava,
      Map(new Integer(Int.MaxValue) -> "hello").asJava,
      Seq.fill(collectionSize)(new java.lang.Long(Long.MaxValue)).toSet.asJava
    )
    scalaRequest = TScalaRequest(
      Int.MaxValue,
      Long.MaxValue,
      false,
      "hello",
      Seq.fill(collectionSize)("hello"),
      Map(Int.MaxValue -> "hello"),
      Seq.fill(collectionSize)(Long.MaxValue).toSet
    )
    // use a mock service to get the request bytes so we don't need to
    // write the request to a thrift buffer manually
    val echoScalaSvc = new Service[ThriftClientRequest, Array[Byte]] {
      def apply(treq: ThriftClientRequest) = {
        thriftRequestBytes = treq.message
        // Throw an exception since it requires us to put a properly
        // serialized response. It is OK to throw an exception since
        // all we want is the serialized request.
        Future.exception(new Exception("boom"))
      }
    }
    val scalaIface =
      new TScalaThriftOneGenServer.FinagledClient(echoScalaSvc, new TBinaryProtocol.Factory())

    // the mock service will throw an exception
    try { Await.result(scalaIface.echo(scalaRequest)) }
    catch { case _: Throwable => }

    scalaServer = {
      val impl = new TScalaThriftOneGenServer.MethodPerEndpoint {
        val scalaResponse = TScalaResponse(
          Int.MaxValue,
          Long.MaxValue,
          false,
          "hello",
          Seq.fill(collectionSize)("hello"),
          Map(Int.MaxValue -> "hello"),
          Seq.fill(collectionSize)(Long.MaxValue).toSet
        )
        def echo(r: TScalaRequest) = Future.value(scalaResponse)
      }
      new TScalaThriftOneGenServer.FinagledService(impl, new TBinaryProtocol.Factory())
    }
    val thriftResponseBytes = Await.result(scalaServer(thriftRequestBytes))

    scalaClient = new TScalaThriftOneGenServer.FinagledClient(
      new Service[ThriftClientRequest, Array[Byte]] {
        def apply(treq: ThriftClientRequest) = {
          Future.value(thriftResponseBytes)
        }
      },
      new TBinaryProtocol.Factory())

    javaServer = {
      val impl = new TJavaThriftOneGenServer.ServiceIface {
        val javaResponse = new TJavaResponse(
          Int.MaxValue,
          Long.MaxValue,
          false,
          "hello",
          Seq.fill(collectionSize)("hello").asJava,
          Map(new Integer(Int.MaxValue) -> "hello").asJava,
          Seq.fill(collectionSize)(new java.lang.Long(Long.MaxValue)).toSet.asJava
        )
        def echo(r: TJavaRequest) = Future.value(javaResponse)
      }
      new TJavaThriftOneGenServer.Service(impl, new TBinaryProtocol.Factory())
    }

    javaClient = new TJavaThriftOneGenServer.ServiceToClient(
      new Service[ThriftClientRequest, Array[Byte]] {
        def apply(req: ThriftClientRequest) = Future.value(thriftResponseBytes)
      },
      new TBinaryProtocol.Factory())
  }

  @Benchmark
  def scala_server(): Array[Byte] = {
    Await.result(scalaServer(thriftRequestBytes))
  }

  @Benchmark
  def scala_client(): TScalaResponse = {
    Await.result(scalaClient.echo(scalaRequest))
  }

  @Benchmark
  def java_server(): Array[Byte] = {
    Await.result(javaServer(thriftRequestBytes))
  }

  @Benchmark
  def java_client(): TJavaResponse = {
    Await.result(javaClient.echo(javaRequest))
  }
}
