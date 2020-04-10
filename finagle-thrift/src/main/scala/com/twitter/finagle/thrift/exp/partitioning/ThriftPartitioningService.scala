package com.twitter.finagle.thrift.exp.partitioning

import com.twitter.finagle
import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.partitioning.param
import com.twitter.finagle.thrift.ThriftClientRequest
import com.twitter.finagle.{ServiceFactory, Stack}

/**
 * ThriftPartitioningService applies [[PartitioningStrategy]] on the Thrift client.
 * If a [[HashingPartitioningStrategy]] is set in Param, a consistent hashing based
 * partitioning will be turned on.
 */
private[finagle] object ThriftPartitioningService {

  case class Param(strategy: PartitioningStrategy)

  object Param {
    implicit val param: Stack.Param[Param] =
      Stack.Param(Param(Disabled))
  }

  val role: Stack.Role = Stack.Role("ThriftPartitioningService")
  val description: String = "Apply partition awareness on the Thrift client."

  def module: Stack.Module[ServiceFactory[ThriftClientRequest, Array[Byte]]] =
    new Stack.Module[ServiceFactory[ThriftClientRequest, Array[Byte]]] {
      val parameters = Seq(
        implicitly[Stack.Param[LoadBalancerFactory.Dest]],
        implicitly[Stack.Param[finagle.param.Stats]]
      )

      final override def make(
        params: Stack.Params,
        next: Stack[ServiceFactory[ThriftClientRequest, Array[Byte]]]
      ): Stack[ServiceFactory[ThriftClientRequest, Array[Byte]]] = {
        params[Param].strategy match {
          case Disabled => next
          case hashingStrategy: HashingPartitioningStrategy =>
            val param.KeyHasher(hasher) = params[param.KeyHasher]
            val param.NumReps(numReps) = params[param.NumReps]
            val service =
              new ThriftHashingPartitioningService(next, params, hashingStrategy, hasher, numReps)
            Stack.leaf(role, ServiceFactory.const(service))
        }
      }
      def role: Stack.Role = ThriftPartitioningService.role
      def description: String = ThriftPartitioningService.description
    }
}
