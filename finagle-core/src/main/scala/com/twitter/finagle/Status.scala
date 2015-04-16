package com.twitter.finagle

import com.twitter.conversions.time._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{Await, Future}
import scala.math.Ordering

/**
 * Status tells the condition of a networked endpoint. They are used
 * to indicate the health of [[Service]], [[ServiceFactory]], and of
 * [[transport.Transport]].
 *
 * Object [[Status$]] contains the status definitions.
 */
sealed trait Status

/**
 * Define valid [[Status!]] values. They are, in order from
 * most to least healthy:
 *
 *  - Open
 *  - Busy
 *  - Closed
 *
 * (An [[scala.math.Ordering]] is defined in these terms.)
 */
object Status {
  private implicit val timer = DefaultTimer.twitter

  class ClosedException extends Exception("Status was Closed; expected Open")

  implicit val StatusOrdering: Ordering[Status] = Ordering.by({
    case Open => 3
    case Busy => 2
    case Closed => 1
  })

  /**
   * A composite status indicating the least healthy of the two.
   */
  def worst(left: Status, right: Status): Status =
    StatusOrdering.min(left, right)

  /**
   * A composite status indicating the most healthy of the two.   
   */
  def best(left: Status, right: Status): Status =
    StatusOrdering.max(left, right)

  /**
   * The status representing the worst of the given statuses
   * extracted by `status` on `ts`.
   *
   * @note this may terminate early so don't rely on this method
   *       for running side effects on `ts`
   */
  def worstOf[T](ts: Iterable[T], status: T => Status): Status = {
    var worst: Status = Status.Open
    val itr = ts.iterator
    while (itr.hasNext && worst != Status.Closed)
      worst = Status.worst(worst, status(itr.next()))
    worst
  }

  /**
   * The status representing the best of the given statuses
   * extracted by `status` on `ts`.
   *
   * @note this may terminate early so don't rely on this method
   *       for running side effects on `ts`
   */
  def bestOf[T](ts: Iterable[T], status: T => Status): Status = {
    var best: Status = Status.Closed
    val itr = ts.iterator
    while (itr.hasNext && best != Status.Open)
      best = Status.best(best, status(itr.next()))
    best
  }

  /**
   * Open returns a [[com.twitter.util.Future]] that is satisfied
   * when the status returned by `get` is [[Open]]. It returns
   * an exceptional [[com.twitter.util.Future]] should it be
   * [[Closed]].
   *
   * `whenOpen` polls the underlying status, using 
   * exponential backoffs from 1ms to around 1s.
   */
  def whenOpen(get: => Status): Future[Unit] = {
    def go(n: Int): Future[Unit] = get match {
      case Open => Future.Done
      case Closed => Future.exception(new ClosedException)
      case Busy => Future.sleep((1<<n).milliseconds) before go(math.min(n+1, 10))
    }

    go(0)
  }
  
  /**
   * A blocking version of [[whenOpen]]; this method returns 
   * when the status has become [[Open]]. This call
   * blocks and should only be used outside of Finagle
   * threads to halt progress until the status is [[Open]].
   *
   * @throws [[ClosedException]] if the status becomes [[Closed]].
   */
  def awaitOpen(get: => Status): Unit =
    Await.result(whenOpen(get))

  /**
   * An open [[Service]] or [[ServiceFactory]] is ready to be used.
   * It can service requests or sessions immediately.
   */
  case object Open extends Status

  /**
   * A busy [[Service]] or [[ServiceFactory]] is transiently
   * unavailable. A Busy [[Service]] or [[ServiceFactory]] can be
   * used, but may not provide service immediately. 
   */
  case object Busy extends Status

  /**
   * The [[Service]] or [[ServiceFactory]] is closed. It will never
   * service requests or sessions again. (And should probably be
   * discarded.)
   */
  case object Closed extends Status
}
