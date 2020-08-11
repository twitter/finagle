package com.twitter.finagle.thrift.exp.partitioning

import com.twitter.finagle
import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.partitioning.param
import com.twitter.finagle.thrift.ThriftClientRequest
import com.twitter.finagle.{FailureFlags, ServiceFactory, Stack}
import com.twitter.logging.{HasLogLevel, Level}

/**
 * ThriftPartitioningService applies [[PartitioningStrategy]] on the Thrift client.
 * If a [[HashingPartitioningStrategy]] is set in Param, a consistent hashing based
 * partitioning will be turned on.
 */
private[finagle] object ThriftPartitioningService {

  case class Strategy(strategy: PartitioningStrategy)

  object Strategy {
    implicit val strategy: Stack.Param[Strategy] =
      Stack.Param(Strategy(Disabled))
  }

  /**
   * Failed to get PartitionIds/HashKeys and Requests from [[PartitioningStrategy]].
   */
  final class PartitioningStrategyException(
    message: String,
    cause: Throwable = null,
    val flags: Long = FailureFlags.Empty)
      extends Exception(message, cause)
      with FailureFlags[PartitioningStrategyException]
      with HasLogLevel {
    def this(cause: Throwable) = this(null, cause)
    def logLevel: Level = Level.ERROR

    protected def copyWithFlags(flags: Long): PartitioningStrategyException =
      new PartitioningStrategyException(message, cause, flags)
  }

  /**
   * A helper class to provide helper methods for different Protocols when marshalling
   * requests and responses. Now distinctions are between Thrift messages and
   * ThriftMux messages.
   */
  trait ReqRepMarshallable[Req, Rep] {

    /**
     * Frames a new Request by using the serialized request content req, and properties
     * in the original request.
     */
    def framePartitionedRequest(thriftClientRequest: ThriftClientRequest, original: Req): Req

    /**
     * Gets the oneway property from the original request.
     * Oneway methods will generate code that does not wait for a response.
     */
    def isOneway(original: Req): Boolean

    /** Gets the serialized data from the response body */
    def fromResponseToBytes(rep: Rep): Array[Byte]

    /** Returns an empty response of the Response type */
    def emptyResponse: Rep
  }

  val role: Stack.Role = Stack.Role("ThriftPartitioningService")
  val description: String = "Apply partition awareness on the Thrift client."

  def module[Req, Rep](
    thriftMarshallable: ReqRepMarshallable[Req, Rep]
  ): Stack.Module[ServiceFactory[Req, Rep]] =
    new Stack.Module[ServiceFactory[Req, Rep]] {
      val parameters = Seq(
        implicitly[Stack.Param[LoadBalancerFactory.Dest]],
        implicitly[Stack.Param[finagle.param.Stats]]
      )

      final override def make(
        params: Stack.Params,
        next: Stack[ServiceFactory[Req, Rep]]
      ): Stack[ServiceFactory[Req, Rep]] = {
        params[Strategy].strategy match {
          case Disabled => next
          case hashingStrategy: HashingPartitioningStrategy =>
            val param.KeyHasher(hasher) = params[param.KeyHasher]
            val param.NumReps(numReps) = params[param.NumReps]
            val service = new ThriftHashingPartitioningService[Req, Rep](
              next,
              thriftMarshallable,
              params,
              hashingStrategy,
              hasher,
              numReps)
            Stack.leaf(role, ServiceFactory.const(service))
          case customStrategy: CustomPartitioningStrategy =>
            val service = new ThriftCustomPartitioningService[Req, Rep](
              next,
              thriftMarshallable,
              params,
              customStrategy
            )
            Stack.leaf(role, ServiceFactory.const(service))
        }
      }
      def role: Stack.Role = ThriftPartitioningService.role
      def description: String = ThriftPartitioningService.description
    }
}
