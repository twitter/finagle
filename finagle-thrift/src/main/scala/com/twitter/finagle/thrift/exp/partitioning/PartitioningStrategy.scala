package com.twitter.finagle.thrift.exp.partitioning

import com.twitter.finagle.partitioning.PartitionNodeManager
import com.twitter.finagle.thrift.exp.partitioning.ClientCustomStrategy.ToPartitionedMap
import com.twitter.finagle.thrift.exp.partitioning.PartitioningStrategy._
import com.twitter.finagle.thrift.exp.partitioning.ThriftPartitioningService.PartitioningStrategyException
import com.twitter.finagle.{Address, ServiceFactory, Stack}
import com.twitter.scrooge.{ThriftMethodIface, ThriftStructIface}
import com.twitter.util.{Activity, Future, Try}
import java.lang.{Integer => JInteger}
import java.util.function.{BiFunction, IntFunction, Function => JFunction}
import java.util.{List => JList, Map => JMap, Set => JSet}
import scala.collection.JavaConverters._
import scala.collection.mutable

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
  private[finagle] def newNodeManager[Req, Rep](
    underlying: Stack[ServiceFactory[Req, Rep]],
    params: Stack.Params
  ): PartitionNodeManager[Req, Rep, _, ClientCustomStrategy.ToPartitionedMap]

  /**
   * A ResponseMergerRegistry implemented by client to supply [[ResponseMerger]]s
   * for message fan-out cases.
   *
   * @see [[ResponseMerger]]
   */
  val responseMergerRegistry: ResponseMergerRegistry = new ResponseMergerRegistry()
}

private[finagle] object Disabled extends PartitioningStrategy

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
   * Constructs a [[ClientCustomStrategy]] that does not reshard.
   *
   * This is appropriate for static partitioning backend topologies.
   *
   * Java users should see [[ClientCustomStrategies$]] for an easier to use API.
   *
   * @param getPartitionIdAndRequest A PartialFunction implemented by client that
   *        provides the partitioning logic on a request. It takes a Thrift object
   *        request, and returns Future Map of partition ids to sub-requests. If
   *        we don't need to fan-out, it should return one element: partition id
   *        to the original request.  This PartialFunction can take multiple
   *        Thrift request types of one Thrift service (different method endpoints
   *        of one service).  In this context, the returned partition id is also
   *        the shard id.  Each instance is its own partition.
   */
  def noResharding(
    getPartitionIdAndRequest: ClientCustomStrategy.ToPartitionedMap
  ): CustomPartitioningStrategy =
    noResharding(getPartitionIdAndRequest, { a: Int => Seq(a) })

  /**
   * Constructs a [[ClientCustomStrategy]] that does not reshard.
   *
   * This is appropriate for static partitioning backend topologies.
   *
   * Java users should see [[ClientCustomStrategies$]] for an easier to use API.
   *
   * @param getPartitionIdAndRequest A PartialFunction implemented by client that
   *        provides the partitioning logic on a request. It takes a Thrift object
   *        request, and returns Future Map of partition ids to sub-requests. If
   *        we don't need to fan-out, it should return one element: partition id
   *        to the original request.  This PartialFunction can take multiple
   *        Thrift request types of one Thrift service (different method endpoints
   *        of one service).
   * @param getLogicalPartitionId Gets the logical partition identifiers from a host
   *        identifier, host identifiers are derived from [[ZkMetadata]]
   *        shardId. Indicates which logical partitions a physical host belongs to,
   *        multiple hosts can belong to the same partition, and one host can belong
   *        to multiple partitions, for example:
   *        {{{
   *          {
   *            case a if Range(0, 10).contains(a) => Seq(0, 1)
   *            case b if Range(10, 20).contains(b) => Seq(1)
   *            case c if Range(20, 30).contains(c) => Seq(2)
   *            case _ => throw ...
   *          }
   *        }}}
   */
  def noResharding(
    getPartitionIdAndRequest: ClientCustomStrategy.ToPartitionedMap,
    getLogicalPartitionId: Int => Seq[Int]
  ): CustomPartitioningStrategy =
    new ClientCustomStrategy[Unit](
      _ => getPartitionIdAndRequest,
      _ => getLogicalPartitionId,
      Activity.value(()))

  /**
   * Constructs a [[ClientCustomStrategy]] that reshards based on the remote cluster state.
   *
   * This is appropriate for simple custom strategies where you only need to
   * know information about the remote cluster in order to reshard. For example,
   * if you want to be able to add or remove capacity safely.
   *
   * Java users should see [[ClientCustomStrategies$]] for an easier to use API.
   *
   * @param getPartitionIdAndRequestFn A function that given the current state of the
   *        remote cluster, returns a function that gets the logical partition
   *        identifier from a host identifier, host identifiers are derived from
   *        [[ZkMetadata]] shardId. Indicates which logical partition a physical
   *        host belongs to, multiple hosts can belong to the same partition, and one host
   *        can belong to multiple partitions, for example:
   *        {{{
   *          {
   *            case a if Range(0, 10).contains(a) => Seq(0, 1)
   *            case b if Range(10, 20).contains(b) => Seq(1)
   *            case c if Range(20, 30).contains(c) => Seq(2)
   *            case _ => throw ...
   *          }
   *        }}}
   *        Note that this function must be pure (ie referentially transparent).
   *        It cannot change based on anything other than the state of the
   *        remote cluster it is provided with, or else it will malfunction.
   */
  def clusterResharding(
    getPartitionIdAndRequestFn: Set[Address] => ClientCustomStrategy.ToPartitionedMap
  ): CustomPartitioningStrategy =
    clusterResharding(getPartitionIdAndRequestFn, _ => { a: Int => Seq(a) })

  /**
   * Constructs a [[ClientCustomStrategy]] that reshards based on the remote cluster state.
   *
   * This is appropriate for simple custom strategies where you only need to
   * know information about the remote cluster in order to reshard. For example,
   * if you want to be able to add or remove capacity safely.
   *
   * Java users should see [[ClientCustomStrategies$]] for an easier to use API.
   *
   * @param getPartitionIdAndRequestFn A function that given the current state of
   *        the remote cluster, returns a PartialFunction implemented by client
   *        that provides the partitioning logic on a request. It takes a Thrift
   *        object request, and returns Future Map of partition ids to
   *        sub-requests. If we don't need to fan-out, it should return one
   *        element: partition id to the original request.  This PartialFunction
   *        can take multiple Thrift request types of one Thrift service
   *        (different method endpoints of one service).  Note that this function
   *        must be pure (ie referentially transparent).  It cannot change
   *        based on anything other than the state of the remote cluster it is
   *        provided with, or else it will malfunction.
   * @param getLogicalPartitionIdFn A function that given the current state of the
   *        remote cluster, returns a function that gets the logical partition
   *        identifiers from a host identifier, host identifiers are derived from
   *        [[ZkMetadata]] shardId. Indicates which logical partitions a physical
   *        host belongs to, multiple hosts can belong to the same partition,
   *        and one host can belong to multiple partitions, for example:
   *        {{{
   *          {
   *            case a if Range(0, 10).contains(a) => Seq(0, 1)
   *            case b if Range(10, 20).contains(b) => Seq(1)
   *            case c if Range(20, 30).contains(c) => Seq(2)
   *            case _ => throw ...
   *          }
   *        }}}
   *        Note that this function must be pure (ie referentially transparent).
   *        It cannot change based on anything other than the state of the
   *        remote cluster it is provided with, or else it will malfunction.
   */
  def clusterResharding(
    getPartitionIdAndRequestFn: Set[Address] => ClientCustomStrategy.ToPartitionedMap,
    getLogicalPartitionIdFn: Set[Address] => Int => Seq[Int]
  ): CustomPartitioningStrategy =
    new ClientClusterStrategy(getPartitionIdAndRequestFn, getLogicalPartitionIdFn)

  /**
   * Constructs a [[ClientCustomStrategy]] that reshards based on the user provided state.
   *
   * This lets the client be aware of the backend dynamic resharding by providing the
   * fully described state of resharding. The partitioning schema needs to be configured
   * to react to each state, and it needs to be a pure function (see param below).
   * When the state got successfully updated, the partitioning strategy will move
   * to the new schema. See [[clusterResharding]] if only the backend cluster information
   * needs to be observed in order to reshard.
   *
   * Java users should see [[ClientCustomStrategies$]] for an easier to use API.
   *
   * @param getPartitionIdAndRequestFn A function that given the current state of
   *        `observable`, returns a PartialFunction implemented by client that
   *        provides the partitioning logic on a request. It takes a Thrift
   *        object request, and returns Future Map of partition ids to
   *        sub-requests. If we don't need to fan-out, it should return one
   *        element: partition id to the original request.  This PartialFunction
   *        can take multiple Thrift request types of one Thrift service
   *        (different method endpoints of one service).  Note that this
   *        function must be pure (ie referentially transparent).  It cannot
   *        change based on anything other than the state of `observable`, or
   *        else it will malfunction.
   * @param observable The state that is used for deciding how to reshard the
   *        cluster.
   */
  def resharding[A](
    getPartitionIdAndRequestFn: A => ClientCustomStrategy.ToPartitionedMap,
    observable: Activity[A]
  ): CustomPartitioningStrategy =
    resharding[A](getPartitionIdAndRequestFn, (_: A) => { a: Int => Seq(a) }, observable)

  /**
   * Constructs a [[ClientCustomStrategy]] that reshards based on the user provided state.
   *
   * This lets the client be aware of the backend dynamic resharding by providing the
   * fully described state of resharding. The partitioning schema needs to be configured
   * to react to each state, and it needs to be a pure function (see param below).
   * When the state got successfully updated, the partitioning strategy will move
   * to the new schema. See [[clusterResharding]] if only the backend cluster information
   * needs to be observed in order to reshard.
   *
   * Java users should see [[ClientCustomStrategies$]] for an easier to use API.
   *
   * @param getPartitionIdAndRequestFn A function that given the current state of
   *        `observable`, returns a PartialFunction implemented by client that
   *        provides the partitioning logic on a request. It takes a Thrift
   *        object request, and returns Future Map of partition ids to
   *        sub-requests. If we don't need to fan-out, it should return one
   *        element: partition id to the original request.  This PartialFunction
   *        can take multiple Thrift request types of one Thrift service
   *        (different method endpoints of one service).  Note that this
   *        function must be pure (ie referentially transparent).  It cannot
   *        change based on anything other than the state of `observable`, or
   *        else it will malfunction.
   * @param getLogicalPartitionIdFn A function that given the current state
   *        `observable`, returns a function that gets the logical partition
   *        identifiers from a host identifier, host identifiers are derived from
   *        [[ZkMetadata]] shardId. Indicates which logical partitions a physical
   *        host belongs to, multiple hosts can belong to the same partition,
   *        and one host can belong to multiple partitions, for example:
   *        {{{
   *          {
   *            case a if Range(0, 10).contains(a) => Seq(0, 1)
   *            case b if Range(10, 20).contains(b) => Seq(1)
   *            case c if Range(20, 30).contains(c) => Seq(2)
   *            case _ => throw ...
   *          }
   *        }}}
   *        Note that this function must be pure (ie referentially transparent).
   *        It cannot change based on anything other than the state of
   *        `observable`, or else it will malfunction.
   * @param observable The state that is used for deciding how to reshard the
   *        cluster.
   */
  def resharding[A](
    getPartitionIdAndRequestFn: A => ClientCustomStrategy.ToPartitionedMap,
    getLogicalPartitionIdFn: A => Int => Seq[Int],
    observable: Activity[A]
  ): CustomPartitioningStrategy =
    new ClientCustomStrategy(getPartitionIdAndRequestFn, getLogicalPartitionIdFn, observable)

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
 * The java-friendly way to create a [[ClientCustomStrategy]].
 * Scala users should instead use the parallel methods on [[ClientCustomStrategy$]].
 *
 * @note [[com.twitter.util.Function]] may be useful in helping create a [[scala.PartialFunction]].
 */
object ClientCustomStrategies {

  type ToPartitionedMap = PartialFunction[
    ThriftStructIface,
    Future[JMap[JInteger, ThriftStructIface]]
  ]

  private[this] def toJavaSet[A]: PartialFunction[Set[A], JSet[A]] = { case set => set.asJava }
  private[this] val toScalaFutureMap: Future[JMap[JInteger, ThriftStructIface]] => Future[
    Map[Int, ThriftStructIface]
  ] = (_.map(_.asScala.toMap.map {
    case (k, v) => (k.toInt, v)
  }))

  /**
   * The java-friendly way to create a [[ClientCustomStrategy]].
   * Scala users should instead use the parallel methods on [[ClientCustomStrategy$]].
   *
   * @note [[com.twitter.util.Function]] may be useful in helping create a [[scala.PartialFunction]].
   */
  def noResharding(toPartitionedMap: ToPartitionedMap): CustomPartitioningStrategy =
    ClientCustomStrategy.noResharding(toPartitionedMap.andThen(toScalaFutureMap))

  /**
   * The java-friendly way to create a [[ClientCustomStrategy]].
   * Scala users should instead use the parallel methods on [[ClientCustomStrategy$]].
   *
   * @note [[com.twitter.util.Function]] may be useful in helping create a [[scala.PartialFunction]].
   */
  def noResharding(
    toPartitionedMap: ToPartitionedMap,
    getLogicalPartitionId: IntFunction[JList[Integer]]
  ): CustomPartitioningStrategy = ClientCustomStrategy.noResharding(
    toPartitionedMap.andThen(toScalaFutureMap),
    getLogicalPartitionId.apply(_).asScala.toSeq.map(_.toInt))

  /**
   * The java-friendly way to create a [[ClientCustomStrategy]].
   * Scala users should instead use the parallel methods on [[ClientCustomStrategy$]].
   *
   * @note [[com.twitter.util.Function]] may be useful in helping create a [[scala.PartialFunction]].
   */
  def clusterResharding(
    getPartitionIdAndRequestFn: JFunction[JSet[Address], ToPartitionedMap]
  ): CustomPartitioningStrategy =
    ClientCustomStrategy.clusterResharding(
      toJavaSet[Address]
        .andThen(getPartitionIdAndRequestFn.apply(_).andThen(toScalaFutureMap)))

  /**
   * The java-friendly way to create a [[ClientCustomStrategy]].
   * Scala users should instead use the parallel methods on [[ClientCustomStrategy$]].
   *
   * @note [[com.twitter.util.Function]] may be useful in helping create a [[scala.PartialFunction]].
   */
  def clusterResharding(
    getPartitionIdAndRequestFn: JFunction[JSet[Address], ToPartitionedMap],
    getLogicalPartitionIdFn: JFunction[JSet[Address], JFunction[Int, JList[Int]]]
  ): CustomPartitioningStrategy =
    ClientCustomStrategy.clusterResharding(
      toJavaSet[Address]
        .andThen(getPartitionIdAndRequestFn.apply(_).andThen(toScalaFutureMap)),
      toJavaSet[Address]
        .andThen(getLogicalPartitionIdFn.apply _).andThen(op => op.apply(_).asScala.toSeq)
    )

  /**
   * The java-friendly way to create a [[ClientCustomStrategy]].
   * Scala users should instead use the parallel methods on [[ClientCustomStrategy$]].
   *
   * @note [[com.twitter.util.Function]] may be useful in helping create a [[scala.PartialFunction]].
   */
  def resharding[A](
    getPartitionIdAndRequestFn: JFunction[A, ToPartitionedMap],
    observable: Activity[A]
  ): CustomPartitioningStrategy =
    ClientCustomStrategy
      .resharding[A](
        { a: A =>
          getPartitionIdAndRequestFn.apply(a).andThen(toScalaFutureMap)
        },
        observable)

  /**
   * The java-friendly way to create a [[ClientCustomStrategy]].
   * Scala users should instead use the parallel methods on [[ClientCustomStrategy$]].
   *
   * @note [[com.twitter.util.Function]] may be useful in helping create a [[scala.PartialFunction]].
   */
  def resharding[A](
    getPartitionIdAndRequestFn: JFunction[A, ToPartitionedMap],
    getLogicalPartitionIdFn: JFunction[A, JFunction[Int, Seq[Int]]],
    observable: Activity[A]
  ): CustomPartitioningStrategy =
    ClientCustomStrategy
      .resharding[A](
        { a: A =>
          getPartitionIdAndRequestFn.apply(a).andThen(toScalaFutureMap)
        },
        { a: A => getLogicalPartitionIdFn.apply(a).apply _ },
        observable)
}

private[partitioning] final class ClientClusterStrategy(
  val getPartitionIdAndRequestFn: Set[Address] => ClientCustomStrategy.ToPartitionedMap,
  val getLogicalPartitionIdFn: Set[Address] => Int => Seq[Int])
    extends CustomPartitioningStrategy {

  // we don't have a real implementation here because the understanding
  // is that implementors will not use ClientClusterStrategy directly, but
  // instead use it to derive a ClientCustomStrategy where the implementors
  // provides access to the remote cluster's state.
  override def newNodeManager[Req, Rep](
    underlying: Stack[ServiceFactory[Req, Rep]],
    params: Stack.Params
  ): PartitionNodeManager[Req, Rep, _, ToPartitionedMap] = ???
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
 * @param getLogicalPartitionId Gets the logical partition identifiers from a host
 *        identifier, host identifiers are derived from [[ZkMetadata]]
 *        shardId. Indicates which logical partitions a physical host belongs to,
 *        multiple hosts can belong to the same partition, and one host can belong to
 *        multiple partitions, for example:
 *        {{{
 *          val getLogicalPartition: Int => Seq[Int] = {
 *            case a if Range(0, 10).contains(a) => Seq(0, 1)
 *            case b if Range(10, 20).contains(b) => Seq(1)
 *            case c if Range(20, 30).contains(c) => Seq(2)
 *            case _ => throw ...
 *          }
 *        }}}
 *        If not provided, the default is that each instance is its own partition.
 * @param observable  The state that is used for deciding how to reshard the cluster.
 */
final class ClientCustomStrategy[A] private[partitioning] (
  val getPartitionIdAndRequest: A => ClientCustomStrategy.ToPartitionedMap,
  val getLogicalPartitionId: A => Int => Seq[Int],
  val observable: Activity[A])
    extends CustomPartitioningStrategy {

  def newNodeManager[Req, Rep](
    underlying: Stack[ServiceFactory[Req, Rep]],
    params: Stack.Params
  ): PartitionNodeManager[Req, Rep, _, ClientCustomStrategy.ToPartitionedMap] =
    new PartitionNodeManager(
      underlying,
      observable,
      getPartitionIdAndRequest,
      getLogicalPartitionId,
      params)

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
 * @param getLogicalPartitionId Gets the logical partition identifiers from a host
 *        identifier, host identifiers are derived from [[ZkMetadata]]
 *        shardId. Indicates which logical partitions a physical host belongs to,
 *        multiple hosts can belong to the same partition, and one host can belong to
 *        multiple partitions, for example:
 *        {{{
 *          val getLogicalPartition: Int => Seq[Int] = {
 *            case a if Range(0, 10).contains(a) => Seq(0, 1)
 *            case b if Range(10, 20).contains(b) => Seq(1)
 *            case c if Range(20, 30).contains(c) => Seq(2)
 *            case _ => throw ...
 *          }
 *        }}}
 *        If not provided, the default is that each instance is its own partition.
 * @param responseMerger  Supplies a [[ResponseMerger]] for messaging fan-out.
 *        Non-fan-out case the default is [[None]].
 */
final class MethodBuilderCustomStrategy[Req <: ThriftStructIface, Rep](
  val getPartitionIdAndRequest: MethodBuilderCustomStrategy.ToPartitionedMap[Req],
  getLogicalPartitionId: Int => Seq[Int],
  val responseMerger: Option[ResponseMerger[Rep]])
    extends CustomPartitioningStrategy {

  def this(
    getPartitionIdAndRequest: MethodBuilderCustomStrategy.ToPartitionedMap[Req],
    getLogicalPartitionId: Int => Seq[Int]
  ) = this(
    getPartitionIdAndRequest,
    getLogicalPartitionId,
    None
  )

  def this(getPartitionIdAndRequest: MethodBuilderCustomStrategy.ToPartitionedMap[Req]) =
    this(getPartitionIdAndRequest, Seq(_))

  def this(
    getPartitionIdAndRequest: MethodBuilderCustomStrategy.ToPartitionedMap[Req],
    responseMerger: Option[ResponseMerger[Rep]]
  ) = this(
    getPartitionIdAndRequest,
    Seq(_),
    responseMerger
  )

  // TODO reconcile these types w/ req / rep
  def newNodeManager[U, T](
    underlying: Stack[ServiceFactory[U, T]],
    params: Stack.Params
  ): PartitionNodeManager[U, T, _, ClientCustomStrategy.ToPartitionedMap] =
    new PartitionNodeManager[U, T, Unit, ClientCustomStrategy.ToPartitionedMap](
      underlying,
      Activity.value(()),
      _ => {
        case req: ThriftStructIface =>
          getPartitionIdAndRequest.asInstanceOf[
            ThriftStructIface => Future[Map[Int, ThriftStructIface]]
          ](req)
      },
      _ => getLogicalPartitionId,
      params
    )

}
