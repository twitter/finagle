package com.twitter.finagle.loadbalancer.aperture

import com.twitter.util.{Event, Witness}

/**
 * [[ProcessCoordinate]] exposes a mechanism that allows a process to be
 * furnished with a coordinate between [0.0, 1.0). The coordinate is calculated relative
 * to other nodes in the process' peer group such that each coordinate is uniformly spaced
 * from another. The result is that each process that is part of the respective group
 * is now part of a topology with a sense of order and proximity.
 */
object ProcessCoordinate {

  /**
   * An ADT which represents the process coordinate.
   */
  sealed trait Coord {
    /**
     * Returns a position between the range [0, 1.0) which represents this
     * process' offset on the shared coordinate space.
     */
    val offset: Double

    /**
     * Returns the uniform width of a peer which is part of this peer group.
     * The returned value is bounded between (0, 1].
     */
    val unitWidth: Double
  }

  /**
   * Defines a coordinate from a process' instance id.
   *
   * The coordinate is calculated between the range [0, 1.0) and is primarily
   * a function of `instanceId` and `totalInstances`. The latter dictates the size of
   * each uniform slice in the coordinate space and the former dictates this process'
   * location on the coordinate space. An additional parameter, `offset`, is exposed
   * that allows clients of the same peer group to coordinate a rotation or offset of
   * the coordinate space in order to avoid alignment with other client groups of
   * the same size (i.e. to add some entropy to the system).
   */
  private[finagle] case class FromInstanceId(peerOffset: Int, instanceId: Int, totalInstances: Int) extends Coord {
    require(totalInstances > 0, s"totalInstances expected to be > 0 but was $totalInstances")

    val unitWidth: Double = 1.0 / totalInstances

    val offset: Double = {
      // compute the offset for this process between [0.0, 1.0).
      val normalizedOffset: Double = peerOffset / Int.MaxValue.toDouble
      val coord = (instanceId * unitWidth + normalizedOffset) % 1.0
      if (coord < 0) coord + 1.0 else coord
    }
  }

  /**
   * An [[Event]] which tracks the current process coordinate.
   */
  private[this] val coordinate: Event[Option[Coord]] with Witness[Option[Coord]] = Event()
  @volatile private[this] var ref: Option[Coord] = None

  /**
   * An [[Event]] which triggers every time the process coordinate changes. This exposes
   * a push based API for the coordinate.
   */
  private[aperture] val changes: Event[Option[Coord]] = coordinate.dedup

  /**
   * Returns the current coordinate, if there is one set.
   */
  def apply(): Option[Coord] = ref

  /**
   * Globally set the coordinate for this process from the respective instance
   * metadata.
   *
   * @param peerOffset A parameter which allows clients of the same peer group to
   * apply a rotation or offset of the coordinate space in order to avoid any correlations
   * between peer groups. A good value for this is a deterministic hash of a shared notion
   * of process identifier (e.g. hash("/s/my-cluster")).
   *
   * @param instanceId An instance identifier for this process w.r.t its peer
   * cluster.
   *
   * @param totalInstances The total number of instances in this process' peer
   * cluster.
   */
  def setCoordinate(peerOffset: Int, instanceId: Int, totalInstances: Int): Unit =
    updateCoordinate(Some(FromInstanceId(peerOffset, instanceId, totalInstances)))

  /**
   * Disables the ordering for this process and forces each Finagle client that
   * uses [[Aperture]] to derive a random ordering.
   */
  def unsetCoordinate(): Unit = updateCoordinate(None)

  private[this] def updateCoordinate(newCoord: Option[Coord]): Unit = {
    // We update the atomic ref directly in this method because we want the most
    // up-to-date information in the `apply()` method and it avoids potential race
    // conditions where updating ref via a witness happens *after* other
    // registered witnesses call the `apply()` method.
    ref = newCoord
    coordinate.notify(newCoord)
  }
}
