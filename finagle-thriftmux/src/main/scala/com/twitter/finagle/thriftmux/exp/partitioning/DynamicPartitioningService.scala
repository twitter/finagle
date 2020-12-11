package com.twitter.finagle.thriftmux.exp.partitioning

import com.twitter.finagle
import com.twitter.finagle.ThriftMux.ThriftMuxMarshallable
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.mux.{Request, Response}
import com.twitter.finagle.partitioning.param
import com.twitter.finagle.thrift.exp.partitioning.{
  CustomPartitioningStrategy,
  Disabled,
  HashingPartitioningStrategy,
  PartitioningStrategy,
  ThriftCustomPartitioningService,
  ThriftHashingPartitioningService
}
import com.twitter.finagle.{Service, ServiceFactory, Stack, Stackable, Status}
import com.twitter.util.{Closable, Future, Time}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import java.util.function.{Function => JFunction}
import scala.collection.JavaConverters._

private[thriftmux] object DynamicPartitioningService {
  private val strategyKey = new Contexts.local.Key[PartitioningStrategy]()

  /**
   * Sets a per-request partitioning strategy, scoped to `f`. It only works in conjunction
   * with a client using the [[perRequestModule]] installed in its stack.
   */
  def letStrategy[T](strategy: PartitioningStrategy)(f: => T): T =
    Contexts.local.let(strategyKey, strategy) { f }

  def perRequestModule: Stackable[ServiceFactory[Request, Response]] =
    new Stack.Module[ServiceFactory[Request, Response]] {
      override def make(
        params: Stack.Params,
        next: Stack[ServiceFactory[Request, Response]]
      ): Stack[ServiceFactory[Request, Response]] = {
        val service = new DynamicPartitioningService(params, next)
        Stack.leaf(role, ServiceFactory.const(service))
      }

      def role: Stack.Role = Stack.Role("DynamicPartitioningService")

      def description: String = "Apply dynamic partition awareness on the ThriftMux MethodBuilder."

      def parameters: Seq[Stack.Param[_]] = Seq(
        implicitly[Stack.Param[LoadBalancerFactory.Dest]],
        implicitly[Stack.Param[finagle.param.Stats]]
      )
    }
}

/**
 * A Service to switch among partitioning services based on the partitioning strategy
 * of each request.
 */
private[partitioning] class DynamicPartitioningService(
  params: Stack.Params,
  next: Stack[ServiceFactory[Request, Response]])
    extends Service[Request, Response] {

  import DynamicPartitioningService._

  private[this] val pool: ConcurrentMap[
    PartitioningStrategy,
    Service[Request, Response]
  ] = new ConcurrentHashMap()

  private[this] val strategyFn =
    new JFunction[PartitioningStrategy, Service[Request, Response]] {
      def apply(strategy: PartitioningStrategy): Service[Request, Response] = {
        strategy match {
          case hashingStrategy: HashingPartitioningStrategy =>
            val param.KeyHasher(hasher) = params[param.KeyHasher]
            val param.NumReps(numReps) = params[param.NumReps]
            new ThriftHashingPartitioningService[Request, Response](
              next,
              ThriftMuxMarshallable,
              params,
              hashingStrategy,
              hasher,
              numReps)
          case customStrategy: CustomPartitioningStrategy =>
            new ThriftCustomPartitioningService[Request, Response](
              next,
              ThriftMuxMarshallable,
              params,
              customStrategy
            )
          case Disabled => next.make(params).toService // should not happen
        }
      }
    }

  // Note, we want to initialize the non-partitioned request path eagerly. This allows
  // features like eager connections to work without priming the client with a request.
  private[this] val disabledPartitioningService = next.make(params).toService

  def apply(request: Request): Future[Response] = {
    val strategy = Contexts.local.getOrElse(strategyKey, () => Disabled)
    val service = strategy match {
      case Disabled => disabledPartitioningService
      case _ => pool.computeIfAbsent(strategy, strategyFn)
    }
    service(request)
  }

  // We don't clear the pool so we won't re-compute a service when
  // the MethodBuilder endpoint is closed.
  override def close(deadline: Time): Future[Unit] = {
    Closable.all(disabledPartitioningService +: pool.values.asScala.toSeq: _*).close(deadline)
  }

  // This status is the MethodBuilder status, it is only Closed if all partitioning
  // services are closed.
  override def status: Status = {
    val services = disabledPartitioningService +: pool.values.asScala.toSeq
    Status.bestOf[Service[Request, Response]](services, service => service.status)
  }

  // exposed for testing
  private[partitioning] def getPool =
    new ConcurrentHashMap[PartitioningStrategy, Service[Request, Response]](pool)
}
