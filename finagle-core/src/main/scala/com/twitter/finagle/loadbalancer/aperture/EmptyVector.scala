package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle.Status
import com.twitter.finagle.loadbalancer.aperture.ProcessCoordinate.Coord
import com.twitter.logging.Logger

/**
 * A distributor which has an aperture size but an empty vector to select
 * from, so it always returns the `failingNode`.
 */
private[aperture] class EmptyVector[Req, Rep, Node <: ApertureNode[Req, Rep]](
  emptyException: Throwable,
  failingNode: (Throwable) => Node,
  override val initAperture: Int,
  _minAperture: => Int,
  override val updateVectorHash: (Vector[Node]) => Unit,
  _dapertureActive: => Boolean,
  _eagerConnections: => Boolean,
  override val mkDeterministicAperture: (Vector[Node], Int, Coord) => BaseDist[Req, Rep, Node],
  override val mkRandomAperture: (Vector[Node], Int) => BaseDist[Req, Rep, Node],
  override val rebuildLog: Logger)
    extends BaseDist[Req, Rep, Node](Vector.empty) {

  require(vector.isEmpty, s"vector must be empty: $vector")

  override def minAperture: Int = _minAperture

  override def dapertureActive: Boolean = _dapertureActive

  override def eagerConnections: Boolean = _eagerConnections

  override val mkEmptyVector: Int => BaseDist[Req, Rep, Node] = (size: Int) => {
    new EmptyVector(
      emptyException,
      failingNode,
      size,
      _minAperture,
      updateVectorHash,
      _dapertureActive,
      _eagerConnections,
      mkDeterministicAperture,
      mkRandomAperture,
      rebuildLog
    )
  }

  def indices: Set[Int] = Set.empty
  def status: Status = Status.Closed
  def pick(): Node = failingNode(emptyException)
  def needsRebuild: Boolean = false
  def additionalMetadata: Map[String, Any] = Map.empty
}
