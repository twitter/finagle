package com.twitter.finagle.thrift

import com.twitter.finagle.context.Contexts
import com.twitter.io.Buf

object Headers {

  private[finagle] object Values {
    def apply(headers: Seq[(Buf, Buf)]): Values = new Values(headers)
  }

  /**
   * A mutable container for "header" like value information.
   *
   * @note  This container is mutable because the information is carried across
   *        different code layers via [[com.twitter.finagle.context.LocalContext]].
   *        Thus, we create a local with a default empty value at the top of the local
   *        scope which is then mutated and read by code within the closure.
   *
   * @note Any subsequent call to `set` replaces the underlying values.
   */
  private[finagle] class Values(initialValues: Seq[(Buf, Buf)]) {
    @volatile private[this] var valuesRef = initialValues

    def set(values: Seq[(Buf, Buf)]): Unit = {
      valuesRef = values
    }

    def values: Seq[(Buf, Buf)] = valuesRef
  }

  /**
   * Container for Request "header" value information. Provides a
   * [[com.twitter.finagle.context.LocalContext]] Key for Request headers as
   * well as an empty header values constant.
   */
  object Request {
    def newValues: Values = Values(Nil)
    val Key: Contexts.local.Key[Headers.Values] = new Contexts.local.Key[Headers.Values]
  }

  /**
   * Container for Response "header" value information. Provides a
   * [[com.twitter.finagle.context.LocalContext]] Key for Response headers as
   * well as an empty header values constant.
   */
  object Response {
    def newValues: Values = Values(Nil)
    val Key: Contexts.local.Key[Headers.Values] = new Contexts.local.Key[Headers.Values]
  }
}
