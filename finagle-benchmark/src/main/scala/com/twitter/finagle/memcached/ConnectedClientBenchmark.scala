package com.twitter.finagle.memcached

import com.twitter.finagle.Service
import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.memcached.protocol.{Response, Value, Values}
import com.twitter.io.Buf
import com.twitter.util.Future
import org.openjdk.jmh.annotations._

class ConnectedClientBenchmark extends StdBenchAnnotations {
  import ConnectedClientBenchmark._

  // to run:
  // sbt 'project finagle-benchmark' 'jmh:run -prof gc ConnectedClientBenchmark.timeGetResult'
  @Benchmark
  def timeGetResult(state: GetResultState): Future[GetResult] = {
    import state._
    client.getResult(state.keys)
  }
}

object ConnectedClientBenchmark {
  @State(Scope.Benchmark)
  class GetResultState {
    val keys = Seq.empty

    @Param(Array("1", "10", "100"))
    var responseSize: Int = 1

    var client: ConnectedClient = _

    @Setup
    def prepare(): Unit = {
      val values = (0 until responseSize).map { i =>
        Value(Buf.Utf8(s"$i"), Buf.Utf8("foo"), None, None)
      }.toList
      val service = Service.const[Response](Future.value(Values(values)))
      client = new ConnectedClient(service)
    }
  }
}
