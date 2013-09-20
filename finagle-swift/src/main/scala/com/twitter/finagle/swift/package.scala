package com.twitter.finagle.exp

import scala.annotation.target.{beanGetter, beanSetter}
import scala.reflect.BeanProperty

/**
 * Package swift makes Facebook's [[https://github.com/facebook/swift
 * Swift]] project work with Finagle. Interfaces annotated with
 * `ThriftService`, whose methods are annotated with `ThriftMethod`
 * implement the corresponding thrift interface. These are used
 * directly with [[com.twitter.finagle.Thrift]] to expose thrift
 * servers or implement thrift clients. A complete example follows.
 *
 * {{{
 * @ThriftService("Test1")
 * trait TestIface {
 *   @ThriftMethod
 *   def ab(
 *       a: String, 
 *       b: java.util.Map[String, java.lang.Integer]
 *   ): Future[java.util.Map[String, java.lang.Integer]]
 *   
 *   @ThriftMethod
 *   def ping(msg: String): Future[String]
 * }
 *
 * val impl = new TestIface { ... }
 *
 * val server = Thrift.serveIface(":*", impl)
 * val client = Thrift.newIface[TestIface](server)
 *
 * }}}
 *
 * `client` is an instance of `TestIface` whose methods are
 * dispatched via thrift, and onto `server`. Currently annotated
 * interfaces must be "java-clean": Scala collections aren't yet
 * supported, and boxed versions of Java primitives must be used
 * explicitly. We may consider adding more Scala awareness to
 * finagle-swift if it proves valuable. Swift-annotated structs may
 * also be used.
 *
 * As with Swift, finagle-swift assigns tag numbers in increasing
 * order, but allows them to be overriden by the `ThriftField`
 * annotation. In
 *
 * {{{
 * trait TestIface {
 *   def concat(@ThriftField(2) a: String, @ThriftField(3) b: String)
 * }
 * }}}
 *
 * tag 2 will be used for the first field, and tag 3 for the second. See
 * the [[https://github.com/facebook/swift Swift documentation]]
 * for more details.
 *
 * @note This package is experimental. Its API may change at any
 * time, or it may be removed entirely.
 */

package object swift {
  type ThriftField = com.facebook.swift.codec.ThriftField @beanGetter @beanSetter
  type ThriftStruct = com.facebook.swift.codec.ThriftStruct
}
