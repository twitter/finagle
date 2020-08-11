package com.twitter.finagle.thrift.exp.partitioning

import com.twitter.finagle.Stack
import com.twitter.finagle.partitioning.param.{EjectFailedHost, KeyHasher, NumReps}
import com.twitter.hashing

/**
 * A collection of methods for configuring the PartitioningService of Thrift clients
 *
 * @tparam A a [[Stack.Parameterized]] client to configure
 */
class PartitioningParams[A <: Stack.Parameterized[A]](self: Stack.Parameterized[A]) {

  /**
   * Produce a Thrift or ThriftMux client with [[PartitioningStrategy]].
   * @param partitioningStrategy see [[PartitioningStrategy]]
   */
  def strategy(partitioningStrategy: PartitioningStrategy): A =
    self.configured(ThriftPartitioningService.Strategy(partitioningStrategy))

  /**
   * Whether to eject failing hosts from the hash ring based on failure accrual.
   * By default, this is off.
   *
   * The failing signal currently is gathered from
   * [[com.twitter.finagle.partitioning.ConsistentHashingFailureAccrualFactory]]
   *
   * @note When turning on, ejection is based on the failure accrual mentioned above, so your
   *       cluster may get different views of the same host. With partitioning strategy updates,
   *       this can introduce inter-process inconsistencies between hash rings.
   *       In many cases, it's better to eject failing host via a separate mechanism that's
   *       based on a global view.
   */
  def ejectFailedHost(eject: Boolean): A = self.configured(EjectFailedHost(eject))

  /**
   * Defines the hash function to use for partitioned clients when
   * mapping keys to partitions.
   */
  def keyHasher(hasher: hashing.KeyHasher): A = self.configured(KeyHasher(hasher))

  /**
   * Duplicate each node across the hash ring according to `reps`.
   *
   * @see [[com.twitter.hashing.ConsistentHashingDistributor]] for more
   * details.
   */
  def numReps(reps: Int): A = self.configured(NumReps(reps))
}
