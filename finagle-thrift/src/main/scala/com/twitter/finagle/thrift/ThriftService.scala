package com.twitter.finagle.thrift

import com.twitter.finagle.Service
import com.twitter.scrooge._

/**
 * A marker trait that signifies that this is a thrift service that can be served by Finagle
 */
trait ThriftService

/**
 * A trait that signifies a `ThriftService` can be created from this
 */
trait ToThriftService {
  def toThriftService: ThriftService
}

/**
 * An abstract class that all scrooge-generated thrift service objects inherit
 * directly from, including services that extend other services.
 */
abstract class GeneratedThriftService {

  /**
   * A type representing an implementation of this service where methods are
   * defined as
   *  `Service[Args, SuccessType]`
   */
  type ServicePerEndpoint <: ToThriftService

  /**
   * A type representing an implementation of this service where methods are
   * defined as
   *  `Service[Request[Args], Response[SuccessType]]`
   */
  type ReqRepServicePerEndpoint <: ToThriftService

  /**
   * A type representing an implementation of this service where methods are
   * defined as
   *  `Args => Future[SuccessType]`
   */
  type MethodPerEndpoint <: ThriftService

  /**
   * Thrift annotations (user-defined key-value metadata) on the service
   */
  def annotations: Map[String, String]

  /**
   * The set of `ThriftMethods` that this service is responsible for handling
   */
  def methods: Set[ThriftMethod]

  /**
   * Generate a `ReqRepServicePerEndpoint` from a map of ThriftMethod ->
   * Service[Request[Args], Response[SuccessType]] implementations. This is
   * unsafe because it does not check that the implementation associated with
   * a ThriftMethod is typed properly until this service attempts to serve a
   * request.
   *
   * @throws IllegalArgumentException if an implementation is missing
   */
  def unsafeBuildFromMethods(
    methods: Map[ThriftMethod, Service[Request[_], Response[_]]]
  ): ReqRepServicePerEndpoint
}
