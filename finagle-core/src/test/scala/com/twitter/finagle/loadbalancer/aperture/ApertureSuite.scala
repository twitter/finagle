package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle._
import com.twitter.finagle.loadbalancer.Balancer
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.util.Rng
import com.twitter.util._
import scala.collection.mutable

trait ApertureSuite {
  class Empty extends Exception

  private[loadbalancer] trait TestBal
    extends Balancer[Unit, Unit]
    with Aperture[Unit, Unit] {

    protected val rng = Rng(12345L)
    protected val emptyException = new Empty
    protected val maxEffort = 5
    protected def statsReceiver = NullStatsReceiver
    protected def minAperture = 1
    protected val useDeterministicOrdering = false

    protected[this] val maxEffortExhausted = statsReceiver.counter("max_effort_exhausted")

    def applyn(n: Int): Unit = {
      val factories = Await.result(Future.collect(Seq.fill(n)(apply())))
      Await.result(Closable.all(factories:_*).close())
    }

    // Expose some protected methods for testing
    def adjustx(n: Int): Unit = adjust(n)
    def aperturex: Int = aperture
    def unitsx: Int = units
    def distx: Distributor = dist
    def rebuildx(): Unit = rebuild()
  }

  case class Factory(i: Int) extends ServiceFactory[Unit, Unit] {
    var n = 0
    var p = 0

    def clear() { n = 0 }

    def apply(conn: ClientConnection): Future[Service[Unit, Unit]] = {
      n += 1
      p += 1
      Future.value(new Service[Unit, Unit] {
        def apply(unit: Unit): Future[Unit] = ???
        override def close(deadline: Time): Future[Unit] = {
          p -= 1
          Future.Done
        }
      })
    }

    @volatile var _status: Status = Status.Open

    override def status: Status = _status
    def status_=(v: Status) { _status = v }

    def close(deadline: Time): Future[Unit] = ???

    override def toString: String = s"Factory(id=$i, requests=$n, status=$status)"
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
    def nonzero: Set[Int] = factories.filter({
      case (_, f) => f.n > 0
    }).keys.toSet


    def apply(i: Int) = factories.getOrElseUpdate(i, Factory(i))

    def range(n: Int): IndexedSeq[ServiceFactory[Unit, Unit]] =
      Vector.tabulate(n) { i => apply(i) }
  }

}