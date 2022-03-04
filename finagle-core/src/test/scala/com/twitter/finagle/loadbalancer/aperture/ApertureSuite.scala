package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle._
import com.twitter.finagle.loadbalancer.EndpointFactory
import com.twitter.finagle.loadbalancer.LoadBalancerFactory.PanicMode
import com.twitter.finagle.util.Rng
import com.twitter.util._
import scala.collection.mutable

private[loadbalancer] trait ApertureSuite {
  class Empty extends Exception

  /**
   * An aperture load balancer which exposes some of the internals
   * via proxy methods.
   */
  trait TestBal extends Aperture[Unit, Unit] {
    private[aperture] val rng = Rng(12345L)
    protected def emptyException = new Empty
    private[aperture] def eagerConnections = false
    private[loadbalancer] val panicMode: PanicMode = PanicMode.MajorityUnhealthy
    private[aperture] def minAperture = 1
    protected val useDeterministicOrdering: Option[Boolean] = None
    val minApertureOverride: Int = 0

    protected def label = ""

    def applyn(n: Int): Unit = {
      val factories = Await.result(Future.collect(Seq.fill(n)(apply())))
      Await.result(Closable.all(factories: _*).close())
    }

    // Expose some protected methods for testing
    def adjustx(n: Int): Unit = adjust(n)
    def aperturex: Int = logicalAperture
    def minUnitsx: Int = minUnits
    def maxUnitsx: Int = maxUnits
    def distx: Distributor = dist
    def rebuildx(): Unit = rebuild()
    def isDeterministicAperture: Boolean =
      dist.isInstanceOf[DeterministicAperture[Unit, Unit, Node]]
    def isRandomAperture: Boolean = dist.isInstanceOf[RandomAperture[Unit, Unit, Node]]
    def isWeightedAperture: Boolean = dist.isInstanceOf[WeightedAperture[Unit, Unit, Node]]
  }

  case class Factory(i: Int) extends EndpointFactory[Unit, Unit] {
    def remake() = {}
    val address: Address = Address.Failed(new Exception)

    var _total = 0
    var _outstanding = 0
    var _numCloses = 0

    /**
     * Returns the total number of services acquired via this factory.
     */
    def total: Int = _total

    /**
     * Returns the current number of outstanding services. Services are
     * relinquished via calls to close.
     */
    def outstanding: Int = _outstanding

    /**
     * The number of times close was called on the factory.
     */
    def numCloses: Int = _numCloses

    /**
     * Clears the total number of services acquired and number of closes.
     */
    def clear(): Unit = {
      _numCloses = 0
      _total = 0
    }

    def apply(conn: ClientConnection): Future[Service[Unit, Unit]] = {
      _total += 1
      _outstanding += 1
      Future.value(new Service[Unit, Unit] {
        def apply(unit: Unit): Future[Unit] = ???
        override def close(deadline: Time): Future[Unit] = {
          _outstanding -= 1
          Future.Done
        }
        override def toString = s"Service($i)"
      })
    }

    @volatile var _status: Status = Status.Open

    override def status: Status = _status
    def status_=(v: Status): Unit = { _status = v }

    def close(deadline: Time): Future[Unit] = {
      _numCloses += 1
      Future.Done
    }

    override def toString: String = s"Factory(id=$i, requests=$total, status=$status)"
  }

  class Counts extends Iterable[Factory] {
    val factories = new mutable.HashMap[Int, Factory]

    def iterator = factories.values.iterator

    def clear(): Unit = {
      factories.values.foreach(_.clear())
    }

    /**
     * This allows test writers to validate the number of [[Counts]] that
     * have received requests. After a statistically significant number
     * of requests sent through the balancer, the size of this collection
     * should be bound by the aperture size.
     */
    def nonzero: Set[Int] =
      factories
        .filter({
          case (_, f) => f.total > 0
        })
        .keys
        .toSet

    def apply(i: Int) = factories.getOrElseUpdate(i, Factory(i))

    def range(n: Int): IndexedSeq[EndpointFactory[Unit, Unit]] =
      Vector.tabulate(n) { i => apply(i) }
  }

}
