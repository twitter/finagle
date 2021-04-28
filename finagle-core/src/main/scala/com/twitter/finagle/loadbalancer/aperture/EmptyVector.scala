package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle.Status

/**
 * A distributor which has an aperture size but an empty vector to select
 * from, so it always returns the `failingNode`.
 */
private class EmptyVector[Req, Rep, NodeT <: ApertureNode[Req, Rep]](
  aperture: Aperture[Req, Rep] { type Node = NodeT },
  initAperture: Int)
    extends BaseDist[Req, Rep, NodeT](aperture, Vector.empty, initAperture) {

  require(vector.isEmpty, s"vector must be empty: $vector")

  def minAperture: Int = aperture.minAperture

  def dapertureActive: Boolean = aperture.dapertureActive

  def eagerConnections: Boolean = aperture.eagerConnections

  def indices: Set[Int] = Set.empty
  def status: Status = Status.Closed
  def pick(): NodeT = aperture.failingNode
  def needsRebuild: Boolean = false
  def additionalMetadata: Map[String, Any] = Map.empty
}
