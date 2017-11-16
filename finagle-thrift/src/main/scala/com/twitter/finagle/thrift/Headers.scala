package com.twitter.finagle.thrift

import com.twitter.finagle.context.Contexts
import com.twitter.finagle.context.Contexts.local.Key
import com.twitter.io.Buf

object Headers {

  private[finagle] object Values {
    def apply(headers: Seq[(Buf, Buf)]): Values = new Values(headers)
  }

  /**
   * A mutable container for "header" like value information.
   *
   * This container is mutable because the information is carried across different code layers
   * via [[com.twitter.finagle.context.LocalContext]]. Thus, we create a local with a default empty
   * value at the top of the local scope which is then mutated and read by code within the closure.
   */
  private[finagle] class Values private(headers: Seq[(Buf, Buf)]) {
    private[this] val _values = scala.collection.mutable.ArrayBuffer[(Buf, Buf)]()
    put(headers)

    def put(headers: Seq[(Buf, Buf)]): Unit = synchronized {
      headers.foreach { value => _values += value }
    }

    def values: Seq[(Buf, Buf)] =  synchronized {
      _values
    }
  }

  /**
   * Container for Request "header" value information. Provides a
   * [[com.twitter.finagle.context.LocalContext]] Key for Request headers as
   * well as an empty header values constant.
   */
  object Request {
    val empty: Values = Values(Seq.empty[(Buf, Buf)])
    val Key: Key[Headers.Values] = new Contexts.local.Key[Headers.Values]
  }

  /**
   * Container for Response "header" value information. Provides a
   * [[com.twitter.finagle.context.LocalContext]] Key for Response headers as
   * well as an empty header values constant.
   */
  object Response {
    val empty: Values = Values(Seq.empty[(Buf, Buf)])
    val Key: Key[Headers.Values] = new Contexts.local.Key[Headers.Values]
  }
}
