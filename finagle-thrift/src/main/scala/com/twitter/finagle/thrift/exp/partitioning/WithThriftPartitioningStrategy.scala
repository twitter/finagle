package com.twitter.finagle.thrift.exp.partitioning

import com.twitter.finagle.Stack

/**
 * Provides the `withPartitioning` API entry point
 *
 * @see [[PartitioningParams]]
 */
trait WithThriftPartitioningStrategy[A <: Stack.Parameterized[A]] { self: Stack.Parameterized[A] =>

  /**
   * An entry point for configuring Thrift/ThriftMux client's partitioning service
   */
  def withPartitioning: PartitioningParams[A] = new PartitioningParams(self)
}
