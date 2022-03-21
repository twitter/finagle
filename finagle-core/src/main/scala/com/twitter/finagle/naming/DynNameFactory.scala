package com.twitter.finagle.naming

import com.twitter.finagle._
import com.twitter.finagle.factory.ServiceFactoryCache
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.util.Activity
import com.twitter.util.Future
import com.twitter.util.Promise
import com.twitter.util.Stopwatch
import com.twitter.util.Time
import scala.collection.immutable

/**
 * Proxies requests to the current definition of 'name', queueing
 * requests while it is pending.
 */
private class DynNameFactory[Req, Rep](
  name: Activity[NameTree[Name.Bound]],
  cache: ServiceFactoryCache[NameTree[Name.Bound], Req, Rep],
  statsReceiver: StatsReceiver = NullStatsReceiver)
    extends ServiceFactory[Req, Rep] {

  val latencyStat = statsReceiver.stat("bind_latency_us")

  private sealed trait State
  private case class Pending(
    q: immutable.Queue[(ClientConnection, Promise[Service[Req, Rep]], Stopwatch.Elapsed)])
      extends State
  private case class Named(name: NameTree[Name.Bound]) extends State
  private case class Failed(exc: Throwable) extends State
  private case class Closed() extends State

  override def status = state match {
    case Pending(_) => Status.Busy
    case Named(name) => cache.status(name)
    case Failed(_) | Closed() => Status.Closed
  }

  @volatile private[this] var state: State = Pending(immutable.Queue.empty)

  private[this] val sub = name.run.changes respond {
    case Activity.Ok(name) =>
      synchronized {
        state match {
          case Pending(q) =>
            state = Named(name)
            for ((conn, p, elapsed) <- q) {
              latencyStat.add(elapsed().inMicroseconds)
              p.become(apply(conn))
            }
          case Failed(_) | Named(_) =>
            state = Named(name)
          case Closed() =>
        }
      }

    case Activity.Failed(exc) =>
      synchronized {
        state match {
          case Pending(q) =>
            for ((_, p, elapsed) <- q) {
              latencyStat.add(elapsed().inMicroseconds)
              p.setException(Failure.adapt(exc, FailureFlags.Naming))
            }
            state = Failed(exc)
          case Failed(_) =>
            // if already failed, just update the exception; the promises
            // must already be satisfied.
            state = Failed(exc)
          case Named(_) | Closed() =>
        }
      }

    case Activity.Pending =>
  }

  def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
    state match {
      case Named(name) =>
        cache(name, conn)

      case Failed(exc) =>
        Future.exception(Failure.adapt(exc, FailureFlags.Naming))

      case Closed() =>
        Future.exception(new ServiceClosedException)

      case Pending(_) =>
        applySync(conn)
    }
  }

  private[this] def applySync(conn: ClientConnection): Future[Service[Req, Rep]] = synchronized {
    state match {
      case Pending(q) =>
        val p = new Promise[Service[Req, Rep]]
        val elapsed = Stopwatch.start()
        val el = (conn, p, elapsed)
        p setInterruptHandler {
          case exc =>
            synchronized {
              state match {
                case Pending(q) if q contains el =>
                  state = Pending(q filter (_ != el))
                  latencyStat.add(elapsed().inMicroseconds)
                  p.setException(new CancelledConnectionException(exc))
                case _ =>
              }
            }
        }
        state = Pending(q enqueue el)
        p

      case _ => apply(conn)
    }
  }

  def close(deadline: Time) = {
    val prev = synchronized {
      val prev = state
      state = Closed()
      prev
    }
    prev match {
      case Pending(q) =>
        val exc = new ServiceClosedException
        for ((_, p, elapsed) <- q) {
          latencyStat.add(elapsed().inMicroseconds)
          p.setException(exc)
        }
      case _ =>
    }
    sub.close(deadline)
  }
}
