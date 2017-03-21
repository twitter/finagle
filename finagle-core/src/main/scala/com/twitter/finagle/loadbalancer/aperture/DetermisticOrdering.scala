package com.twitter.finagle.loadbalancer.aperture

import com.twitter.util.{Event, Witness}
import java.util.concurrent.atomic.AtomicReference

/**
 * [[DeterministicOrdering]] exposes a mechanism that allows a process to be
 * furnished with a coordinate which is used to derive ordering in an [[Aperture]]
 * implementation. The coordinate is calculated relative to other nodes in the process'
 * peer group such that each coordinate is uniformly spaced from another given a shared
 * coordinate space (in our case, [-1.0, 1.0]). The result is that each process that is
 * part of the respective group is now part of a topology with a sense of order and
 * proximity.
 *
 * A unique permutation for a group of servers (that [[Apertures]] expand over) can be
 * then derived by mapping the servers to the same coordinate space and iterating through
 * the space using a uniform traversal (see the [[Iterator Iterators]] defined on [[Ring]]
 * for more details). Thus, when the group of peers converges on an aperture size, the
 * servers are equally represented across the peer's aperture.
 *
 * This remedies a shortcoming with the default [[Aperture]] implementation where peers
 * randomly select the servers in their aperture and, consequently, create load imbalances
 * since servers have a well understood probability of being non-uniformly represented
 * across client apertures.
 *
 * @note To use this, the coordinate needs to be set via `setCoordinate` and the
 * implementation used by the client must have the `useDeterministicOrdering` set
 * to true.
 */
object DeterministicOrdering {
  /**
   * An [[Event]] which tracks the current process coordinate.
   */
  private[this] val coordinate: Event[Option[Double]] with Witness[Option[Double]] = Event()
  private[this] val ref: AtomicReference[Option[Double]] = new AtomicReference(None)
  coordinate.register(Witness(ref))

  /**
   * An [[Event]] which triggers every time the process coordinate changes. This exposes
   * a push based API for the coordinate.
   */
  val changes: Event[Option[Double]] = coordinate.dedup

  /**
   * Returns the current coordinate, if there is one set.
   */
  def apply(): Option[Double] = ref.get

  /**
   * Globally set the coordinate for this process.
   *
   * The coordinate is calculated between the range [-1.0, 1.0] and is primarily
   * a function of `instanceId` and `totalInstances`. The latter dictates the size of
   * each uniform slice in the coordinate space and the former dictates this process'
   * location on the coordinate space. An additional parameter, `offset`, is exposed
   * that allows clients of the same peer group to coordinate a rotation or offset of
   * the coordinate space in order to avoid alignment with other client groups of
   * the same size.
   *
   * @param offset A parameter which allows clients of the same peer group to
   * coordinate a rotation or offset of the coordinate space in order to avoid alignment
   * with other client groups of the same size. A good value for this is the hashCode of
   * a shared notion of process identifier (e.g. "/s/my-group-of-process".hashCode).
   *
   * @param instanceId An instance identifier for this process w.r.t its peer
   * cluster.
   *
   * @param totalInstances The total number of instances in this process' peer
   * cluster.
   */
  def setCoordinate(offset: Int, instanceId: Int, totalInstances: Int): Unit = {
    require(totalInstances > 0, "totalInstances must be > 0")
    val unit: Double = 1.0D / totalInstances
    val normalizedOffset: Double = offset / Int.MaxValue.toDouble
    val coord: Double = (instanceId * unit + normalizedOffset) % 1.0D
    coordinate.notify(Some(coord))
  }

  /**
   * Disables the ordering for this process and forces each Finagle client that
   * uses [[Aperture]] to derive a random ordering.
   */
  def unsetCoordinate(): Unit = {
    coordinate.notify(None: Option[Double])
  }
}