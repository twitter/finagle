package com.twitter.finagle.thrift.exp.partitioning

import com.twitter.finagle.thrift.exp.partitioning.PartitioningStrategy._
import com.twitter.scrooge.{ThriftMethodIface, ThriftStructIface}
import com.twitter.util.Try
import scala.collection.mutable

private[partitioning] object PartitioningStrategy {

  /**
   *
   * A request merger defines the merger function needed to define a Finagle partitioning
   * service for Thrift services.
   *
   * This is used for [[HashingPartitioningStrategy]], which applies a consistent
   * hashing partitioning strategy under the hood. Note that more than one request may
   * fall in the same HashNode, even though they're assigned with different hashing keys.
   * It merges a collection of requests to a final request which
   * matches the Thrift IDL definition. RequestMerger is registered through
   * [[RequestMergerRegistry]]
   *
   * for example:
   * {{{
   *   i32 example(1: string in)
   *
   *   val requestMerger: RequestMerger[Example.Args] = listArgs =>
   *     Example.Args(in = listArgs.map(_.in).mkString(";"))
   * }}}
   */
  type RequestMerger[Req <: ThriftStructIface] = Seq[Req] => Req

  /**
   * A response merger defines the merger function needed to define a Finagle partitioning
   * service for Thrift services.
   *
   * This is used for messaging fan-out, it merges a collection of successful
   * responses and a collection of failures to a final response which matches
   * the Thrift IDL definition. ResponseMerger is registered through
   * [[ResponseMergerRegistry]].
   *
   * for example:
   * {{{
   *   string example(1: i32 a)
   *
   *   val responseMerger: ResponseMerger[String] = (successes, failures) =>
   *     if (successes.nonEmpty) Return(successes.mkString(";"))
   *     else Throw(failures.head)
   * }}}
   */
  type ResponseMerger[Rep] = (Seq[Rep], Seq[Throwable]) => Try[Rep]

  object RequestMergerRegistry {

    /**
     * Create an empty RequestMergerRegistry.
     * @note The created RequestMergerRegistry is NOT thread safe, it carries an assumption
     *       that registries are written during client initialization.
     */
    def create: RequestMergerRegistry = new RequestMergerRegistry()
  }

  /**
   * Maintain a map of method name to method's [[RequestMerger]].
   */
  class RequestMergerRegistry {

    // reqMerger Map is not thread safe, assuming `add` only be called during client
    // initialization and the reqMerger remains the same as request threads `get` from it.
    private[this] val reqMergers: mutable.Map[String, RequestMerger[ThriftStructIface]] =
      mutable.Map.empty

    /**
     * Register a RequestMerger for a ThriftMethod.
     * @param method  ThriftMethod is a method endpoint defined in a thrift service
     * @param merger  see [[RequestMerger]]
     */
    def add[Req <: ThriftStructIface](
      method: ThriftMethodIface,
      merger: RequestMerger[Req]
    ): RequestMergerRegistry = {
      reqMergers += (method.name -> merger.asInstanceOf[RequestMerger[ThriftStructIface]])
      this
    }

    /**
     * Get a RequestMerger for a ThriftMethod.
     * @param methodName  The Thrift method name
     */
    def get(methodName: String): Option[RequestMerger[ThriftStructIface]] =
      reqMergers.get(methodName)
  }

  object ResponseMergerRegistry {

    /**
     * Create an empty ResponseMergerRegistry.
     * @note The created ResponseMergerRegistry is NOT thread safe, it carries an assumption
     *       that registries are written during client initialization.
     */
    def create: ResponseMergerRegistry = new ResponseMergerRegistry()
  }

  /**
   * Maintain a map of method name to method's [[ResponseMerger]].
   */
  class ResponseMergerRegistry {

    // repMergers Map is not thread safe, assuming `add` is only called during client
    // initialization and the repMergers remains the same as request threads `get` from it.
    private[this] val repMergers: mutable.Map[String, ResponseMerger[Any]] = mutable.Map.empty

    /**
     * Register a ResponseMerger for a ThriftMethod.
     * @param method  ThriftMethod is a method endpoint defined in a thrift service
     * @param merger  see [[ResponseMerger]]
     */
    def add[Rep](method: ThriftMethodIface, merger: ResponseMerger[Rep]): ResponseMergerRegistry = {
      repMergers += (method.name -> merger.asInstanceOf[ResponseMerger[Any]])
      this
    }

    /**
     * Get a ResponseMerger for a ThriftMethod.
     * @param methodName  The Thrift method name
     */
    def get(methodName: String): Option[ResponseMerger[Any]] =
      repMergers.get(methodName)
  }
}

/**
 * Service partitioning strategy to apply on the clients in order to let clients route
 * requests accordingly. Two particular partitioning strategies are going to be supported,
 * [[HashingPartitioningStrategy]] and [[CustomPartitioningStrategy]].
 * Either one will need developers to provide a concrete function to give each request an
 * indicator of destination, for example a hashing key or a partition address.
 * Messaging fan-out is supported by leveraging RequestMerger and ResponseMerger.
 */
sealed trait PartitioningStrategy

private[partitioning] object HashingPartitioningStrategy {

  /**
   * Thrift requests not specifying hashing keys will fall in here. This allows a
   * Thrift/ThriftMux partition aware client to serve a part of endpoints of a service.
   * Un-specified endpoints should not be called from this client, otherwise, throw
   * [[com.twitter.finagle.partitioning.ConsistentHashPartitioningService.NoPartitioningKeys]].
   */
  val defaultHashingKeyAndRequest: ThriftStructIface => Map[Any, ThriftStructIface] = args =>
    Map(None -> args)
}

private[partitioning] sealed trait HashingPartitioningStrategy extends PartitioningStrategy
private[partitioning] object Disabled extends PartitioningStrategy

/**
 * Experimental API to set a consistent hashing partition strategy for a Thrift/ThriftMux Client.
 */
private[partitioning] abstract class ClientHashingStrategy extends HashingPartitioningStrategy {
  // input: original thrift request
  // output: a Map of hashing keys and split requests
  type ToPartitionedMap = PartialFunction[ThriftStructIface, Map[Any, ThriftStructIface]]

  /**
   * A PartialFunction implemented by client that provides the partitioning logic on
   * a request. It takes a Thrift object request, and returns a Map of hashing keys to
   * sub-requests. If no fan-out needs, it should return one element: hashing key to the
   * original request.
   * This PartialFunction can take multiple Thrift request types of one Thrift service
   * (different method endpoints of one service). In consideration of messaging fan-out,
   * the PartialFunction returns a map from a hashing key to a request.
   */
  def getHashingKeyAndRequest: ToPartitionedMap

  /**
   * A RequestMergerRegistry implemented by client to supply [[RequestMerger]]s.
   * @see [[RequestMerger]]
   */
  def requestMergerRegistry: RequestMergerRegistry = RequestMergerRegistry.create

  /**
   * A ResponseMergerRegistry implemented by client to supply [[ResponseMerger]]s.
   * @see [[ResponseMerger]]
   */
  def responseMergerRegistry: ResponseMergerRegistry = ResponseMergerRegistry.create
}
