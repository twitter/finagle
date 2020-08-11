package com.twitter.finagle.thrift.exp.partitioning

import com.twitter.finagle.thrift.exp.partitioning.PartitioningStrategy._
import com.twitter.finagle.thrift.exp.partitioning.ThriftPartitioningService.PartitioningStrategyException
import com.twitter.scrooge.{ThriftMethodIface, ThriftStructIface}
import com.twitter.util.{Future, Try}
import java.lang.{Integer => JInteger}
import java.util.function.{BiFunction, Function => JFunction, IntUnaryOperator}
import java.util.{List => JList, Map => JMap}
import scala.collection.mutable
import scala.collection.JavaConverters._

object PartitioningStrategy {

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
   * Failing sub-responses are expected to be handled by the application (proper logging,
   * exception handling, etc),
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

  /**
   * Maintain a map of method name to method's [[RequestMerger]].
   */
  final class RequestMergerRegistry private[partitioning] {

    // reqMerger Map is not thread safe, assuming `add` only be called during client
    // initialization and the reqMerger remains the same as request threads `get` from it.
    private[this] val reqMergers: mutable.Map[String, RequestMerger[ThriftStructIface]] =
      mutable.Map.empty

    /**
     * Register a RequestMerger for a ThriftMethod.
     *
     * For a Java-friendly way to do the same thing, see `addResponseMerger`
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
     * Register a RequestMerger for a ThriftMethod.
     *
     * The same as add, but easier to use from Java.
     * @param method  ThriftMethod is a method endpoint defined in a thrift service
     * @param merger  see [[RequestMerger]]
     */
    def addRequestMerger[Req <: ThriftStructIface](
      method: ThriftMethodIface,
      // Needed for 2.11 compat. can be a scala fn A => B once we drop support
      merger: JFunction[JList[Req], Req]
    ): RequestMergerRegistry = add(method, { seq: Seq[Req] => seq.asJava }.andThen(merger.apply _))

    /**
     * Get a RequestMerger for a ThriftMethod.
     * @param methodName  The Thrift method name
     */
    private[finagle] def get(methodName: String): Option[RequestMerger[ThriftStructIface]] =
      reqMergers.get(methodName)
  }

  /**
   * Maintain a map of method name to method's [[ResponseMerger]].
   */
  final class ResponseMergerRegistry private[partitioning] {

    // repMergers Map is not thread safe, assuming `add` is only called during client
    // initialization and the repMergers remains the same as request threads `get` from it.
    private[this] val repMergers: mutable.Map[String, ResponseMerger[Any]] = mutable.Map.empty

    /**
     * Register a ResponseMerger for a ThriftMethod.
     *
     * For a Java-friendly way to do the same thing, see `addResponseMerger`
     * @param method  ThriftMethod is a method endpoint defined in a thrift service
     * @param merger  see [[ResponseMerger]]
     */
    def add[Rep](method: ThriftMethodIface, merger: ResponseMerger[Rep]): ResponseMergerRegistry = {
      repMergers += (method.name -> merger.asInstanceOf[ResponseMerger[Any]])
      this
    }

    /**
     * Register a ResponseMerger for a ThriftMethod.
     *
     * The same as add, but easier to use from Java.
     * @param method  ThriftMethod is a method endpoint defined in a thrift service
     * @param merger  see [[ResponseMerger]]
     */
    def addResponseMerger[Rep](
      method: ThriftMethodIface,
      merger: BiFunction[JList[Rep], JList[Throwable], Try[Rep]]
    ): ResponseMergerRegistry =
      add(
        method,
        { (reps: Seq[Rep], errs: Seq[Throwable]) =>
          merger.apply(reps.asJava, errs.asJava)
        })

    /**
     * Get a ResponseMerger for a ThriftMethod.
     * @param methodName  The Thrift method name
     */
    private[finagle] def get(methodName: String): Option[ResponseMerger[Any]] =
      repMergers.get(methodName)
  }
}

/**
 * Service partitioning strategy to apply on the clients in order to let clients route
 * requests accordingly. Two particular partitioning strategies are going to be supported,
 * [[HashingPartitioningStrategy]] and [[CustomPartitioningStrategy]], each one supports
 * both configuring Finagle Client Stack and ThriftMux MethodBuilder.
 * Either one will need developers to provide a concrete function to give each request an
 * indicator of destination, for example a hashing key or a partition address.
 * Messaging fan-out is supported by leveraging RequestMerger and ResponseMerger.
 */
sealed trait PartitioningStrategy

sealed trait HashingPartitioningStrategy extends PartitioningStrategy

sealed trait CustomPartitioningStrategy extends PartitioningStrategy {

  /**
   * Gets the logical partition identifier from a host identifier, host identifiers are derived
   * from [[ZkMetadata]] shardId. Indicates which logical partition a physical host belongs to,
   * multiple hosts can belong to the same partition, for example:
   * {{{
   *  val getLogicalPartition: Int => Int = {
   *    case a if Range(0, 10).contains(a) => 0
   *    case b if Range(10, 20).contains(b) => 1
   *    case c if Range(20, 30).contains(c) => 2
   *    case _ => throw ...
   *  }
   * }}}
   */
  def getLogicalPartition(instance: Int): Int
}

private[partitioning] object Disabled extends PartitioningStrategy

object ClientHashingStrategy {
  // input: original thrift request
  // output: a Map of hashing keys and split requests
  type ToPartitionedMap = PartialFunction[ThriftStructIface, Map[Any, ThriftStructIface]]

  /**
   * The Java-friendly way to create a [[ClientHashingStrategy]].
   * Scala users should construct a [[ClientHashStrategy]] directly.
   *
   * @note [[com.twitter.util.Function]] may be useful in helping create a [[scala.PartialFunction]].
   */
  def create(
    toPartitionedMap: PartialFunction[
      ThriftStructIface,
      JMap[Any, ThriftStructIface]
    ]
  ): ClientHashingStrategy = new ClientHashingStrategy(toPartitionedMap.andThen(_.asScala.toMap))

  /**
   * Thrift requests not specifying hashing keys will fall in here. This allows a
   * Thrift/ThriftMux partition aware client to serve a part of endpoints of a service.
   * Un-specified endpoints should not be called from this client, otherwise, throw
   * [[com.twitter.finagle.partitioning.ConsistentHashPartitioningService.NoPartitioningKeys]].
   */
  private[finagle] val defaultHashingKeyAndRequest: ThriftStructIface => Map[
    Any,
    ThriftStructIface
  ] = args => Map(None -> args)
}

/**
 * An API to set a consistent hashing partitioning strategy for a Thrift/ThriftMux Client.
 * For a Java-friendly way to do the same thing, see `ClientHashingStrategy.create`
 *
 * @param getHashingKeyAndRequest A PartialFunction implemented by client that
 *        provides the partitioning logic on a request. It takes a Thrift object
 *        request, and returns a Map of hashing keys to sub-requests. If we
 *        don't need to fan-out, it should return one element: hashing key to
 *        the original request.  This PartialFunction can take multiple Thrift
 *        request types of one Thrift service (different method endpoints of one
 *        service).
 */
final class ClientHashingStrategy(
  val getHashingKeyAndRequest: ClientHashingStrategy.ToPartitionedMap)
    extends HashingPartitioningStrategy {

  /**
   * A RequestMergerRegistry implemented by client to supply [[RequestMerger]]s
   * for message fan-out cases.
   * @see [[RequestMerger]]
   */
  val requestMergerRegistry: RequestMergerRegistry = new RequestMergerRegistry()

  /**
   * A ResponseMergerRegistry implemented by client to supply [[ResponseMerger]]s
   * for message fan-out cases.
   * @see [[ResponseMerger]]
   */
  val responseMergerRegistry: ResponseMergerRegistry = new ResponseMergerRegistry()
}

object MethodBuilderHashingStrategy {
  // input: original thrift request
  // output: a Map of hashing keys and split requests
  type ToPartitionedMap[Req] = Req => Map[Any, Req]
}

/**
 * An API to set a hashing partitioning strategy for a client MethodBuilder.
 * For a Java-friendly way to do the same thing, see `MethodBuilderHashingStrategy.create`
 *
 * @param getHashingKeyAndRequest A function for the partitioning logic. MethodBuilder is
 *                                customized per-method so that this method only takes one
 *                                Thrift request type.
 * @param requestMerger           Supplies a [[RequestMerger]] for messaging fan-out.
 *                                Non-fan-out case the default is [[None]].
 * @param responseMerger          Supplies a [[ResponseMerger]] for messaging fan-out.
 *                                Non-fan-out case the default is [[None]].
 */
final class MethodBuilderHashingStrategy[Req <: ThriftStructIface, Rep](
  val getHashingKeyAndRequest: MethodBuilderHashingStrategy.ToPartitionedMap[Req],
  val requestMerger: Option[RequestMerger[Req]],
  val responseMerger: Option[ResponseMerger[Rep]])
    extends HashingPartitioningStrategy {

  def this(getHashingKeyAndRequest: MethodBuilderHashingStrategy.ToPartitionedMap[Req]) =
    this(getHashingKeyAndRequest, None, None)
}

object ClientCustomStrategy {
  // input: original thrift request
  // output: Future Map of partition ids and split requests
  type ToPartitionedMap = PartialFunction[ThriftStructIface, Future[Map[Int, ThriftStructIface]]]

  /**
   * The java-friendly way to create a [[ClientCustomStrategy]].
   * Scala users should construct a [[ClientCustomStrategy]] directly.
   *
   * @note [[com.twitter.util.Function]] may be useful in helping create a [[scala.PartialFunction]].
   */
  def create(
    toPartitionedMap: PartialFunction[
      ThriftStructIface,
      Future[JMap[JInteger, ThriftStructIface]]
    ]
  ): ClientCustomStrategy = new ClientCustomStrategy(
    toPartitionedMap.andThen(_.map(_.asScala.toMap.map { case (k, v) => (k.toInt, v) })))

  /**
   * The java-friendly way to create a [[ClientCustomStrategy]].
   * Scala users should construct a [[ClientCustomStrategy]] directly.
   *
   * @note [[com.twitter.util.Function]] may be useful in helping create a [[scala.PartialFunction]].
   */
  def create(
    toPartitionedMap: PartialFunction[
      ThriftStructIface,
      Future[JMap[JInteger, ThriftStructIface]]
    ],
    logicalPartitionFn: IntUnaryOperator
  ): ClientCustomStrategy = new ClientCustomStrategy(
    toPartitionedMap.andThen(_.map(_.asScala.toMap.map { case (k, v) => (k.toInt, v) })),
    logicalPartitionFn.applyAsInt _)

  /**
   * Thrift requests not specifying partition ids will fall in here. This allows a
   * Thrift/ThriftMux partition aware client to serve a part of endpoints of a service.
   * Un-specified endpoints should not be called from this client, otherwise, throw
   * [[com.twitter.finagle.thrift.exp.partitioning.ThriftPartitioningService.PartitioningStrategyException]].
   */
  private[finagle] val defaultPartitionIdAndRequest: ThriftStructIface => Future[
    Map[Int, ThriftStructIface]
  ] = { _ =>
    Future.exception(
      new PartitioningStrategyException(
        "An unspecified endpoint has been applied to the partitioning service, please check " +
          "your ClientCustomStrategy.getPartitionIdAndRequest see if the endpoint is defined"))
  }
}

/**
 * An API to set a custom partitioning strategy for a Thrift/ThriftMux Client.
 * For a Java-friendly way to do the same thing, see `ClientCustomStrategy.create`
 *
 * @param getPartitionIdAndRequest A PartialFunction implemented by client that
 *        provides the partitioning logic on a request. It takes a Thrift object
 *        request, and returns Future Map of partition ids to sub-requests. If
 *        we don't need to fan-out, it should return one element: partition id
 *        to the original request.  This PartialFunction can take multiple
 *        Thrift request types of one Thrift service (different method endpoints
 *        of one service).
 * @param logicalPartitionFn Gets the logical partition identifier from a host
 *        identifier, host identifiers are derived from [[ZkMetadata]]
 *        shardId. Indicates which logical partition a physical host belongs to,
 *        multiple hosts can belong to the same partition, for example:
 *        {{{
 *          val getLogicalPartition: Int => Int = {
 *            case a if Range(0, 10).contains(a) => 0
 *            case b if Range(10, 20).contains(b) => 1
 *            case c if Range(20, 30).contains(c) => 2
 *            case _ => throw ...
 *          }
 *        }}}
 *        If not provided, the default is that each instance is its own partition.
 *
 * @note  When updating the partition topology dynamically, there is a potential one-time
 *        mismatch if a Service Discovery update happens after getPartitionIdAndRequest.
 */
final class ClientCustomStrategy(
  val getPartitionIdAndRequest: ClientCustomStrategy.ToPartitionedMap,
  logicalPartitionFn: Int => Int)
    extends CustomPartitioningStrategy {

  def this(getPartitionIdAndRequest: ClientCustomStrategy.ToPartitionedMap) =
    this(getPartitionIdAndRequest, identity[Int])

  def getLogicalPartition(instance: Int): Int = logicalPartitionFn(instance)

  /**
   * A ResponseMergerRegistry implemented by client to supply [[ResponseMerger]]s
   * for message fan-out cases.
   * @see [[ResponseMerger]]
   */
  val responseMergerRegistry: ResponseMergerRegistry = new ResponseMergerRegistry()
}

object MethodBuilderCustomStrategy {
  // input: original thrift request
  // output: Future Map of partition ids and split requests
  type ToPartitionedMap[Req] = Req => Future[Map[Int, Req]]
}

/**
 * An API to set a custom partitioning strategy for a client MethodBuilder.
 *
 * @param getPartitionIdAndRequest A function for the partitioning logic.
 *        MethodBuilder is customized per-method so that this method only takes one
 *        Thrift request type.
 * @param logicalPartitionFn Gets the logical partition identifier from a host
 *        identifier, host identifiers are derived from [[ZkMetadata]]
 *        shardId. Indicates which logical partition a physical host belongs to,
 *        multiple hosts can belong to the same partition, for example:
 *        {{{
 *          val getLogicalPartition: Int => Int = {
 *            case a if Range(0, 10).contains(a) => 0
 *            case b if Range(10, 20).contains(b) => 1
 *            case c if Range(20, 30).contains(c) => 2
 *            case _ => throw ...
 *          }
 *        }}}
 *        If not provided, the default is that each instance is its own partition.
 * @param responseMerger  Supplies a [[ResponseMerger]] for messaging fan-out.
 *        Non-fan-out case the default is [[None]].
 */
final class MethodBuilderCustomStrategy[Req <: ThriftStructIface, Rep](
  val getPartitionIdAndRequest: MethodBuilderCustomStrategy.ToPartitionedMap[Req],
  logicalPartitionFn: Int => Int,
  val responseMerger: Option[ResponseMerger[Rep]])
    extends CustomPartitioningStrategy {

  def this(
    getPartitionIdAndRequest: MethodBuilderCustomStrategy.ToPartitionedMap[Req],
    logicalPartitionFn: Int => Int
  ) = this(
    getPartitionIdAndRequest,
    logicalPartitionFn,
    None
  )

  def this(getPartitionIdAndRequest: MethodBuilderCustomStrategy.ToPartitionedMap[Req]) =
    this(getPartitionIdAndRequest, identity(_))

  def this(
    getPartitionIdAndRequest: MethodBuilderCustomStrategy.ToPartitionedMap[Req],
    responseMerger: Option[ResponseMerger[Rep]]
  ) = this(
    getPartitionIdAndRequest,
    identity[Int],
    responseMerger
  )

  def getLogicalPartition(instance: Int): Int = logicalPartitionFn(instance)
}
