package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle.Status
import com.twitter.finagle.loadbalancer.DistributorT
import com.twitter.finagle.loadbalancer.aperture.ProcessCoordinate.Coord
import com.twitter.logging.{Level, Logger}
import com.twitter.util.Future

/**
 * A distributor which implements the logic for controlling the size of an aperture
 * but defers the implementation of pick to concrete implementations.
 */
private[aperture] abstract class BaseDist[Req, Rep, Node <: ApertureNode[Req, Rep]](
  vector: Vector[Node])
    extends DistributorT[Node](vector) {

  val initAperture: Int

  def minAperture: Int

  val updateVectorHash: (Vector[Node]) => Unit

  val mkEmptyVector: (Int) => BaseDist[Req, Rep, Node]

  def dapertureActive: Boolean

  def eagerConnections: Boolean

  val mkDeterministicAperture: (Vector[Node], Int, Coord) => BaseDist[Req, Rep, Node]

  val mkRandomAperture: (Vector[Node], Int) => BaseDist[Req, Rep, Node]

  val rebuildLog: Logger

  type This = BaseDist[Req, Rep, Node]

  /**
   * Returns the maximum size of the aperture window.
   */
  final def max: Int = vector.size

  /**
   * Returns the minimum size of the aperture window.
   */
  def min: Int = math.min(minAperture, vector.size)

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

  def rebuild(vec: Vector[Node]): This = {
    rebuilt = true

    updateVectorHash(vec)
    val dist = if (vec.isEmpty) {
      mkEmptyVector(initAperture)
    } else if (dapertureActive) {
      ProcessCoordinate() match {
        case Some(coord) =>
          mkDeterministicAperture(vec, initAperture, coord)
        case None =>
          // this should not happen as `dapertureActive` should prevent this case
          // but hypothetically, the coordinate could get unset between calls
          // to `dapertureActive` and `ProcessCoordinate()`
          mkRandomAperture(vec, initAperture)
      }
    } else {
      mkRandomAperture(vec, initAperture)
    }

    if (eagerConnections) {
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
  private def doEagerlyConnect(oldNodes: Set[Node]): Unit = {
    val is = indices
    if (rebuildLog.isLoggable(Level.DEBUG)) {
      val newEndpoints = is.count(i => !oldNodes.contains(vector(i)))
      rebuildLog.debug(s"establishing ${newEndpoints} eager connections")
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
