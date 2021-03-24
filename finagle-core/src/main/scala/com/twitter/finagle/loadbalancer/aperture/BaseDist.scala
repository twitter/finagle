package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle.Status
import com.twitter.finagle.loadbalancer.DistributorT
import com.twitter.logging.Level
import com.twitter.util.Future

/**
 * A distributor which implements the logic for controlling the size of an aperture
 * but defers the implementation of pick to concrete implementations.
 */
private[aperture] abstract class BaseDist[Req, Rep, NodeT <: ApertureNode[Req, Rep]](
  aperture: Aperture[Req, Rep] { type Node = NodeT },
  vector: Vector[NodeT],
  initAperture: Int)
    extends DistributorT[NodeT](vector) {
  type This = BaseDist[Req, Rep, NodeT]

  /**
   * Returns the maximum size of the aperture window.
   */
  final def max: Int = vector.size

  /**
   * Returns the minimum size of the aperture window.
   */
  def min: Int = math.min(aperture.minAperture, vector.size)

  // We are guaranteed that writes to aperture are serialized since
  // we only expose them via the `narrow`, `widen`, etc. methods above. Those
  // defer to the balancers `updater` which is serial. Therefore, we only
  // need to guarantee visibility across threads and don't need to
  // provide other synchronization between threads.
  @volatile private[this] var _logicalAperture: Int = initAperture
  // Make sure the aperture is within bounds [min, max].
  adjust(0)

  /**
   * Returns the current logical aperture.
   */
  def logicalAperture: Int = _logicalAperture

  /**
   * Represents how many servers `pick` will select over – which may
   * differ from `logicalAperture` when using [[DeterministicAperture]].
   */
  def physicalAperture: Int = logicalAperture

  /**
   * Adjusts the logical aperture by `n` while ensuring that it stays
   * within the bounds [min, max].
   */
  final def adjust(n: Int): Unit = {
    _logicalAperture = math.max(min, math.min(max, _logicalAperture + n))
  }

  /**
   * A flag indicating that this distributor has been discarded due to a rebuild.
   */
  @volatile private var rebuilt: Boolean = false

  final def rebuild(): This = rebuild(vector)

  def rebuild(vec: Vector[NodeT]): This = {
    rebuilt = true

    aperture.updateVectorHash(vec)
    val dist = if (vec.isEmpty) {
      aperture.mkEmptyVector(initAperture)
    } else if (aperture.dapertureActive) {
      ProcessCoordinate() match {
        case Some(coord) =>
          aperture.mkDeterministicAperture(vec, initAperture, coord)
        case None =>
          // this should not happen as `dapertureActive` should prevent this case
          // but hypothetically, the coordinate could get unset between calls
          // to `dapertureActive` and `ProcessCoordinate()`
          aperture.mkRandomAperture(vec, initAperture)
      }
    } else {
      aperture.mkRandomAperture(vec, initAperture)
    }

    if (aperture.eagerConnections) {
      val oldNodes = indices.map(vector(_))
      dist.doEagerlyConnect(oldNodes)
    }

    dist
  }

  /**
   * Eagerly connects to the endpoints within the aperture. The connections created are
   * out of band without any timeouts. If an in-flight request picks a host that has no
   * established sessions, a request-driven connection will be established.
   */
  private def doEagerlyConnect(oldNodes: Set[NodeT]): Unit = {
    val is = indices
    if (aperture.rebuildLog.isLoggable(Level.DEBUG)) {
      val newEndpoints = is.count(i => !oldNodes.contains(vector(i)))
      aperture.rebuildLog.debug(s"establishing ${newEndpoints} eager connections")
    }

    is.foreach { i =>
      val node = vector(i)
      if (!oldNodes.contains(node)) {
        ApertureEagerConnections.submit {
          if (rebuilt) Future.Done
          else node().flatMap(svc => svc.close())
        }
      }
    }
  }

  /**
   * Returns the indices which are currently part of the aperture. That is,
   * the indices over which `pick` selects.
   */
  def indices: Set[Int]

  /*
   * Returns the best status of nodes within `indices`
   */
  def status: Status

  def additionalMetadata: Map[String, Any]
}
