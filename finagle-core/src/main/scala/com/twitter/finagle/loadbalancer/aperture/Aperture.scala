package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle._
import com.twitter.finagle.loadbalancer.{Balancer, DistributorT}
import com.twitter.finagle.util.{Ring, Rng}
import scala.collection.immutable.VectorBuilder
import scala.collection.mutable.ListBuffer

private object Aperture {
  // Note, we need to have a non-zero range for each node
  // in order for Ring.pick2 to pick distinctly. That is,
  // `RingWidth` should be wider than the number of slices
  // in the ring.
  private val RingWidth = Int.MaxValue

  // Ring that maps to 0 for every value.
  private val ZeroRing = Ring(1, RingWidth)
}

/**
 * The aperture distributor balances load onto a window--the
 * aperture--of underlying capacity. The distributor exposes a
 * control mechanism so that a controller can adjust the aperture
 * according to load conditions.
 *
 * The window contains a number of discrete serving units, one for each
 * node. No load metric is prescribed: this can be mixed in separately.
 *
 * The underlying nodes are arranged in a consistent fashion: an
 * aperture of a given size always refers to the same set of nodes; a
 * smaller aperture to a subset of those nodes. Thus it is relatively
 * harmless to adjust apertures frequently, since underlying nodes
 * are typically backed by pools, and will be warm on average.
 */
private[loadbalancer] trait Aperture[Req, Rep] { self: Balancer[Req, Rep] =>
  import Aperture._

  protected def rng: Rng

  /**
   * The minimum allowable aperture. Must be greater than zero.
   */
  protected def minAperture: Int

  private[this] val gauge = statsReceiver.addGauge("aperture") { aperture }

  protected class Distributor(vector: Vector[Node], initAperture: Int)
    extends DistributorT[Node](vector) {
    type This = Distributor

    private[this] val (ring, unitWidth, maxAperture, minAperture) =
      if (vector.isEmpty) {
        (ZeroRing, RingWidth, RingWidth, 0)
      } else {
        val numNodes = vector.size
        val ring = Ring(numNodes, RingWidth)
        val unit = RingWidth / numNodes
        val max = RingWidth / unit

        // The logic of pick() assumes that the aperture size is less than or
        // equal to number of available nodes, and may break if that is not true.
        val min = math.min(
          math.min(Aperture.this.minAperture, max),
          numNodes
        )

        (ring, unit, max, min)
      }

    @volatile private[Aperture] var aperture = initAperture

    // Make sure the aperture is within bounds [1, maxAperture].
    adjust(0)

    protected[Aperture] def adjust(n: Int) {
      aperture = math.max(minAperture, math.min(maxAperture, aperture + n))
    }

    def rebuild(): This = rebuild(vector)

    def rebuild(vector: Vector[Node]): This = {
      // We need to sort the nodes, with priority being given to the most
      // healthy nodes, then by token. There is an race condition with the
      // sort: the status can change before the sort is finished but this
      // is ignored as if the transition simply happened immediately after
      // the rebuild.

      // The token is immutable, so no race condition here.
      val byToken = vector.sortBy(_.token)

      // We bring the most healthy nodes to the front.
      val resultNodes = new VectorBuilder[Node]
      val busyNodes = new ListBuffer[Node]
      val closedNodes = new ListBuffer[Node]

      byToken.foreach { node =>
        node.status match {
          case Status.Open   => resultNodes += node
          case Status.Busy   => busyNodes += node
          case Status.Closed => closedNodes += node
        }
      }

      resultNodes ++= busyNodes ++= closedNodes

      new Distributor(resultNodes.result, aperture)
    }

    /**
     * The number of available serving units.
     */
    def units: Int = maxAperture

    // We use power of two choices to pick nodes. This keeps things
    // simple, but we could reasonably use a heap here, too.
    def pick(): Node = {
      if (vector.isEmpty)
        return failingNode(emptyException)

      if (vector.size == 1)
        return vector(0)

      val (i, j) = ring.pick2(rng, 0, aperture * unitWidth)
      val a = vector(i)
      val b = vector(j)

      // If both nodes are in the same health status, we pick the least loaded
      // one. Otherwise we pick the one that's healthier.
      if (a.status == b.status) {
        if (a.load < b.load) a else b
      } else {
        if (Status.best(a.status, b.status) == a.status) a else b
      }
    }

    // Since Aperture is probabilistic (it uses P2C) in its selection,
    // we don't partition and select only from healthy nodes. Instead, we
    // rely on the near zero probability of selecting two down nodes (given
    // the layers of retries above us). However, namers can still force
    // rebuilds when the underlying set of nodes changes (eg: some of the
    // nodes were unannounced and restarted).
    def needsRebuild: Boolean = false
  }

  protected def initDistributor(): Distributor =
    new Distributor(Vector.empty, 1)

  /**
   * Adjust the aperture by `n` serving units.
   */
  protected def adjust(n: Int): Unit = invoke(_.adjust(n))

  /**
   * Widen the aperture by one serving unit.
   */
  protected def widen(): Unit = adjust(1)

  /**
   * Narrow the aperture by one serving unit.
   */
  protected def narrow(): Unit = adjust(-1)

  /**
   * The current aperture. This is never less than 1, or more
   * than `units`.
   */
  protected def aperture: Int = dist.aperture

  /**
   * The number of available serving units.
   * The maximum aperture size.
   */
  protected def units: Int = dist.units
}