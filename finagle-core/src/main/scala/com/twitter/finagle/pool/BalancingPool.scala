package com.twitter.finagle.pool

import com.twitter.finagle._
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.client.StackClient
import com.twitter.finagle.loadbalancer.Balancers
import com.twitter.finagle.loadbalancer.EndpointFactory
import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.param
import com.twitter.util.Activity
import com.twitter.util.Closable
import com.twitter.util.Future
import com.twitter.util.Time

/**
 * [[BalancingPool]] is a client [[Stack]] module which composes a collection of
 * [[com.twitter.finagle.pool.SingletonPool]]s with a load balancer. Effectively, this allows a
 * client to load balance over a collection of persistent sessions to the same host/endpoint. This
 * is useful for pipelining or multiplexing protocols that may incur head-of-line blocking (e.g.
 * from the server's processing threads or the network) without this replication.
 */
private[finagle] object BalancingPool {

  /**
   * This module acts as a pool to ensure that it comes before any of the per-session modules
   * in the stack.
   */
  val role: Stack.Role = StackClient.Role.pool

  /**
   * A class eligible for configuring the size of this pool or replication factor. In other words,
   * this controls the number of sessions that will be open and persistent.
   */
  case class Size(size: Int) {
    require(size >= 1, s"count must be >= 1 but was $size")
    def mk(): (Size, Stack.Param[Size]) =
      (this, Size.param)
  }

  object Size {
    implicit val param = Stack.Param(Size(1))
  }

  /**
   * The load balancing primitives we have built-in to Finagle operate over the concept of an
   * "endpoint". Since we reuse them here, we need to define our node which we load balance over
   * in terms of an [[com.twitter.finagle.loadbalancer.EndpointFactory]].
   */
  case class PoolNode[Req, Rep](underlying: ServiceFactory[Req, Rep], address: Address)
      extends ServiceFactoryProxy[Req, Rep](underlying)
      with EndpointFactory[Req, Rep] {
    // PoolNodes don't have the ability to be "rebuilt", so this is a nop.
    override def remake(): Unit = {}
  }

  /**
   * Creates a collection of [[PoolNode]]s of size `size`.
   */
  private def mkNodes[Req, Rep](
    newPool: () => ServiceFactory[Req, Rep],
    addr: Address,
    size: Int
  ): IndexedSeq[PoolNode[Req, Rep]] = {
    IndexedSeq.tabulate(size) { i =>
      val a = addr match {
        // Note we inject an id into the addresses metadata map to make each
        // replicated address structurally unique. This isn't strictly necessary
        // in this context, but likely a useful property that will avoid us getting
        // bitten if we dedupe these in any of the consumers below.
        case Address.Inet(ia, metadata) =>
          Address.Inet(ia, metadata + ("replicated_address_id" -> i))
        case address => address
      }
      PoolNode(newPool(), a)
    }
  }

  private object IllegalEmptyState
      extends NoBrokersAvailableException(
        name = "balancing_pool",
        baseDtabFn = () => Dtab.empty,
        localDtabFn = () => Dtab.empty,
        limitedDtabFn = () => Dtab.empty
      ) {
    override def exceptionMessage: String = "PoolNodes should be non-empty!"
  }

  /**
   * A load balancer in its materialized form (i.e. [[ServiceFactory]]) which manages its
   * underlying resources when closed. This is important since the default implementation
   * relegates this responsibility to the `TrafficDistributor`.
   */
  class ResourceManagedBal[Req, Rep](
    val balancerFactory: LoadBalancerFactory,
    val nodes: IndexedSeq[PoolNode[Req, Rep]],
    params: Stack.Params)
      extends ServiceFactory[Req, Rep] {
    private val balancer = balancerFactory.newBalancer(
      endpoints = Activity.value(nodes),
      emptyException = IllegalEmptyState,
      params)

    def apply(conn: ClientConnection): Future[Service[Req, Rep]] = balancer()

    def close(deadline: Time): Future[Unit] =
      balancer
        .close(deadline)
        .before(Closable.all(nodes: _*).close(deadline))

    override def toString: String = s"ResourceManagedBal(nodes=$nodes)"
  }

  /**
   * Creates a [[com.twitter.finagle.Stackable]] which load balances over a collection of
   * [[SingletonPool]]s of size [[Size]].
   *
   * @param allowInterrupts Threaded through to [[SingletonPool]]. See [[SingletonPool.module]]
   * for more details on the semantics of this value.
   */
  def module[Req, Rep](allowInterrupts: Boolean): Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module[ServiceFactory[Req, Rep]] {
      val role = BalancingPool.role
      val description = "Maintain a pool of persistent connections which are load balanced over"
      val parameters = Seq(
        implicitly[Stack.Param[Size]],
        implicitly[Stack.Param[Transporter.EndpointAddr]],
        implicitly[Stack.Param[param.Stats]]
      )

      def make(params: Stack.Params, next: Stack[ServiceFactory[Req, Rep]]) = {
        val size = params[Size].size
        if (size == 1) {
          // if we don't have anything to load balance over, we insert a SingletonPool into
          // the stack (by prepending it to `next`).
          SingletonPool.module(allowInterrupts).toStack(next)
        } else {
          val addr = params[Transporter.EndpointAddr].addr

          // Note, this creates a new singleton pool for each replicated address
          val newPool = () => SingletonPool.module(allowInterrupts).toStack(next).make(params)
          val poolNodes = mkNodes(newPool, addr, size)

          assert(poolNodes.nonEmpty, s"$poolNodes size expected to be >= 1")

          // We default to p2cPeakEwma since it's highly sensitive to latency variations.
          // This is acceptable in this context since we won't unevenly load an endpoint
          // by disproportionally choosing one of its replicated sessions.
          val balancerFactory = Balancers.p2cPeakEwma()
          val balancer: ServiceFactory[Req, Rep] =
            new ResourceManagedBal(balancerFactory, poolNodes, params)

          Stack.leaf(this, balancer)
        }
      }
    }

}
