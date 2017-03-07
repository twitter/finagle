package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle._
import com.twitter.finagle.loadbalancer.p2c.P2CPick
import com.twitter.finagle.loadbalancer.{Balancer, DistributorT}
import com.twitter.finagle.util.Rng
import scala.collection.immutable.VectorBuilder
import scala.collection.mutable.ListBuffer

/**
 * The aperture distributor balances load onto a window, the
 * aperture, of underlying capacity. The distributor exposes a
 * control mechanism so that a controller can adjust the aperture
 * according to load conditions.
 *
 * The window contains a number of discrete serving units, one for each
 * node. No load metric is prescribed: this can be mixed in separately.
 *
 * The underlying nodes are arranged in a consistent fashion: an
 * aperture of a given size always refers to the same set of nodes; a
 * smaller aperture to a subset of those nodes so long as the nodes are of
 * equal `status` (i.e. unhealthy nodes are de-prioritized). Thus, it is
 * relatively harmless to adjust apertures frequently, since underlying nodes
 * are typically backed by pools, and will be warm on average.
 */
private[loadbalancer] trait Aperture[Req, Rep] { self: Balancer[Req, Rep] =>

  protected def rng: Rng

  /**
   * The minimum allowable aperture. Must be greater than zero.
   */
  protected def minAperture: Int

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

  private[this] val gauge = statsReceiver.addGauge("aperture") { aperture }

  protected class Distributor(vector: Vector[Node], initAperture: Int)
    extends DistributorT[Node](vector)
    with P2CPick[Node] {

    type This = Distributor

    private[this] val max = vector.size
    private[this] val min = math.min(minAperture, vector.size)

    @volatile private[Aperture] var aperture: Int = initAperture
    private[Aperture] def adjust(n: Int): Unit = {
      aperture = math.max(min, math.min(max, aperture + n))
    }

    // Make sure the aperture is within bounds [minAperture, maxAperture].
    adjust(0)

    protected def rng: Rng = self.rng
    protected def bound: Int = aperture
    protected def emptyNode = failingNode(emptyException)

    /**
     * The number of available serving units.
     */
    def units: Int = max

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

    // Since Aperture is probabilistic (it uses P2C) in its selection,
    // we don't partition and select only from healthy nodes. Instead, we
    // rely on the near zero probability of selecting two down nodes (given
    // the layers of retries above us). However, namers can still force
    // rebuilds when the underlying set of nodes changes (eg: some of the
    // nodes were unannounced and restarted).
    def needsRebuild: Boolean = false
  }

  protected def initDistributor(): Distributor = new Distributor(Vector.empty, 1)
}