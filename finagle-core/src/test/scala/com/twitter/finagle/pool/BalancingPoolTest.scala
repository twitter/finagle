package com.twitter.finagle.pool

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.service.FailingFactory
import com.twitter.util.Await
import com.twitter.util.Awaitable
import com.twitter.util.Future
import com.twitter.util.Time
import java.net.InetAddress
import java.net.InetSocketAddress
import org.scalatest.funsuite.AnyFunSuite

object BalancingPoolTest {
  def await[T](a: Awaitable[T]): T = Await.result(a, 1.second)

  val endpointAddr = Address(new InetSocketAddress(InetAddress.getLoopbackAddress, 8080))

  val nil: Stack[ServiceFactory[Unit, Unit]] = Stack.leaf(
    Stack.Role("nil stack"),
    new FailingFactory[Unit, Unit](new Exception("nil stack"))
  )

  class NodeModule {
    var dispatches: Map[Int, Int] = Map.empty
    var ids: Int = 0
    var closes: Int = 0

    def module: Stackable[ServiceFactory[Unit, Unit]] =
      new Stack.Module0[ServiceFactory[Unit, Unit]] {
        def role: Stack.Role = Stack.Role("node")
        def description: String = "node test module"
        def make(next: ServiceFactory[Unit, Unit]) = {
          new ServiceFactory[Unit, Unit] {
            val id = { ids += 1; ids }
            def apply(conn: ClientConnection): Future[Service[Unit, Unit]] = {
              val incr = dispatches.getOrElse(id, 0) + 1
              dispatches += (id -> incr)
              Future.exception(new Exception("fail"))
            }
            def close(deadline: Time): Future[Unit] = {
              closes += 1
              Future.Done
            }
            def status: Status = Status.Open
          }
        }
      }
  }
}

class BalancingPoolTest extends AnyFunSuite {
  import BalancingPoolTest._
  import BalancingPool.ResourceManagedBal

  test("negative size fails fast") {
    val params = intercept[IllegalArgumentException] {
      Stack.Params.empty + BalancingPool.Size(-1)
    }
  }

  test("size of 1 is a nop") {
    val module = BalancingPool.module[Unit, Unit](allowInterrupts = true)
    val stk = module.toStack(nil)
    // non-materialized stack contains a balancing pool.
    assert(stk.head.role == module.role)

    // when we materialize it with a size of 1, we end up
    // with a vanilla singleton pool.
    val params = Stack.Params.empty + BalancingPool.Size(1)
    val sf = stk.make(params)
    assert(sf.isInstanceOf[SingletonPool[Unit, Unit]])
  }

  test("materializes into a ResourceManagedBal") {
    val module = BalancingPool.module[Unit, Unit](allowInterrupts = true)
    val stk = module.toStack(nil)
    val numNodes = 10
    val params = Stack.Params.empty +
      BalancingPool.Size(numNodes) +
      Transporter.EndpointAddr(endpointAddr)
    val sf = stk.make(params)
    val rmb = sf.asInstanceOf[ResourceManagedBal[Unit, Unit]]
    assert(rmb.nodes.size == numNodes)
    assert(rmb.balancerFactory.toString == "P2CPeakEwma")
  }

  test("balances to underlying nodes") {
    val ctx = new NodeModule
    val numNodes = 10
    val params = Stack.Params.empty +
      BalancingPool.Size(numNodes) +
      Transporter.EndpointAddr(endpointAddr)
    val module = BalancingPool.module[Unit, Unit](allowInterrupts = true)
    val stk = module.toStack(ctx.module.toStack(nil))
    val sf = stk.make(params)
    assert(ctx.ids == numNodes)

    // Hopefully we converge here! Since we can't thread in the rng
    // into the balancer we assume that with enough iterations we will
    // touch all the nodes.
    (0 to 1000).foreach { _ => intercept[Exception] { await(sf()) } }
    assert(ctx.dispatches.size == numNodes)
  }

  test("properly manages node resources") {
    val ctx = new NodeModule
    val numNodes = 10
    val params = Stack.Params.empty +
      BalancingPool.Size(numNodes) +
      Transporter.EndpointAddr(endpointAddr)
    val module = BalancingPool.module[Unit, Unit](allowInterrupts = true)
    val stk = module.toStack(ctx.module.toStack(nil))
    val sf = stk.make(params)
    assert(ctx.ids == numNodes)
    await(sf.close())
    assert(ctx.closes == numNodes)
  }
}
