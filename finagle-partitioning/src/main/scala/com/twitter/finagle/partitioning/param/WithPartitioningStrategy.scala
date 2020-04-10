package com.twitter.finagle.partitioning.param

import com.twitter.finagle.Stack
import com.twitter.hashing

/**
 * A collection of methods for configuring partitioning strategies of Memcached clients
 */
trait WithPartitioningStrategy[A <: Stack.Parameterized[A]] { self: Stack.Parameterized[A] =>

  /**
   * Whether to eject failing hosts from the hash ring based on failure accrual.
   * By default, this is off.
   *
   * The failing signal currently is gathered from
   * [[com.twitter.finagle.partitioning.ConsistentHashingFailureAccrualFactory]]
   *
   * @note: When turning on, ejection is based on the failure accrual mentioned above, so your
   *       cluster may get different views of the same host. With partitioning strategy updates,
   *       this can introduce inter-process inconsistencies between hash rings.
   *       In many cases, it's better to eject failing host via a separate mechanism that's
   *       based on a global view.
   */
  def withEjectFailedHost(eject: Boolean): A =
    configured(EjectFailedHost(eject))

  /**
   * Defines the hash function to use for partitioned clients when
   * mapping keys to partitions.
   */
  def withKeyHasher(hasher: hashing.KeyHasher): A =
    configured(KeyHasher(hasher))

  /**
   * Duplicate each node across the hash ring according to `reps`.
   *
   * @see [[com.twitter.hashing.ConsistentHashingDistributor]] for more
   * details.
   */
  def withNumReps(reps: Int): A =
    configured(NumReps(reps))
}
