package com.twitter.finagle.http

import com.twitter.finagle.benchmark.StdBenchAnnotations
import io.netty.handler.codec.http.QueryStringEncoder
import java.nio.charset.Charset
import java.util.{List => JList, Map => JMap}
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
class ParamMapBenchmark extends StdBenchAnnotations {

  private var queryString: String = _

  private var params: Iterable[(String, String)] = _

//  @Param(Array("1", "10", "100"))
  @Param(Array("10"))
  var size: Int = 1

  @Setup
  def prepare(): Unit = {
    queryString = (0 until size)
      .map { i => s"key$i=value$i" }.mkString("?", "&", "")

    params = (0 until size).map { i =>
      // TODO: this is a bit of a cheat since we fastpath values without a space in the new impl
      s"key$i" -> s"value$i"
    }.toMap
  }

  @Benchmark
  def queryParamDecoder(): JMap[String, JList[String]] = {
    QueryParamDecoder.decode(queryString)
  }

  @Benchmark
  def queryParamEncoder(): String = {
    QueryParamEncoder.encode(params)
  }

  @Benchmark
  def nettyQueryParamEncoder(): String = {
    // copy of the old Netty based implementation
    val encoder = new QueryStringEncoder("", Charset.forName("utf-8"))
    params.foreach {
      case (k, v) =>
        encoder.addParam(k, v)
    }

    encoder.toString
  }

}
