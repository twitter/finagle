package com.twitter.finagle.factory

import com.twitter.finagle._
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.util.{  Activity, Closable, Future, Promise,   Time, Var}
import java.net.SocketAddress
import scala.collection.immutable

/**
 * Proxies requests to the current definiton of 'name', queueing
 * requests while it is pending.
 */
private class DynNameFactory[Req, Rep](
    name: Activity[Name.Bound],
    newService: (Name.Bound, ClientConnection) => Future[Service[Req, Rep]])
  extends ServiceFactory[Req, Rep] {

  private sealed trait State
  private case class Pending(q: immutable.Queue[(ClientConnection, Promise[Service[Req, Rep]])])
    extends State
  private case class Named(name: Name.Bound) extends State
  private case class Failed(exc: Throwable) extends State
  private case class Closed() extends State

  @volatile private[this] var state: State = Pending(immutable.Queue.empty)

  private[this] val sub = name.run.changes respond {
    case Activity.Ok(name) => synchronized {
      state match {
        case Pending(q) =>
          state = Named(name)
          for ((conn, p) <- q) p.become(this(conn))
        case Failed(_) | Named(_) =>
          state = Named(name)
        case Closed() => //
      }
    }

    case Activity.Failed(exc) => synchronized {
      state match {
        case Pending(q) =>
          for ((_, p) <- q) p.setException(exc)
          state = Failed(exc)
        case Failed(_) =>
          state = Failed(exc)
        case Named(_) | Closed() =>
      }
    }

    case Activity.Pending =>
  }

  def apply(conn: ClientConnection) = state match {
    case Named(name) => newService(name, conn)
    case Closed() => Future.exception(new ServiceClosedException)
    case Failed(exc) => Future.exception(exc)
    case Pending(_) => applySync(conn)
  }

  private[this] def applySync(conn: ClientConnection) = synchronized {
    state match {
      case Pending(q) =>
        val p = new Promise[Service[Req, Rep]]
        val el = (conn, p)
        p setInterruptHandler { case exc =>
          synchronized {
            state match {
              case Pending(q) if q contains el =>
                state = Pending(q filter (_ != el))
                p.setException(new CancelledConnectionException(exc))
              case _ =>
            }
          }
        }
        state = Pending(q enqueue el)
        p

      case other => apply(conn)
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
        for ((_, p) <- q)
          p.setException(exc)
      case _ => //
    }
    sub.close(deadline)
  }
}

/**
 * A factory that routes to the local binding of the passed-in
 * [[com.twitter.finagle.Name.Path Name.Path]]. It calls `newFactory`
 * to mint a new [[com.twitter.finagle.ServiceFactory
 * ServiceFactory]] for novel name evaluations.
 *
 * A two-level caching scheme is employed for efficiency:
 *
 * First, Name-trees are evaluated by the default evaluation
 * strategy, which produces a set of [[com.twitter.finagle.Name.Bound
 * Name.Bound]]; these name-sets are cached individually so that they
 * can be reused. Should different name-tree evaluations yield the
 * same name-set, they will use the same underlying (cached) factory.
 *
 * Secondly, in order to avoid evaluating names unnecessarily, we
 * also cache the evaluation relative to a [[com.twitter.finagle.Dtab
 * Dtab]]. This is done to short-circuit the evaluation process most
 * of the time (as we expect most requests to share a namer).
 *
 * @bug This is far too complicated, though it seems necessary for
 * efficiency when namers are occasionally overriden.
 *
 * @bug 'isAvailable' has a funny definition.
 */
private[finagle] class BindingFactory[Req, Rep](
    path: Path,
    newFactory: Var[Addr] => ServiceFactory[Req, Rep],
    statsReceiver: StatsReceiver = NullStatsReceiver,
    maxNameCacheSize: Int = 8,
    maxNamerCacheSize: Int = 4)
  extends ServiceFactory[Req, Rep] {

  private[this] val tree = NameTree.Leaf(path)

  private[this] val nameCache =
    new ServiceFactoryCache[Name.Bound, Req, Rep](
      bound => newFactory(bound.addr),
      statsReceiver.scope("namecache"), maxNameCacheSize)

  private[this] def noBrokersAvailableException = {
    val name = BindingFactory.showWithDtabLocal(path)
    new NoBrokersAvailableException(name)
  }

  private[this] val dtabCache = {
    val newFactory: Dtab => ServiceFactory[Req, Rep] = { dtab =>
      val namer = dtab orElse Namer.global
      val name: Activity[Name.Bound] = namer.bind(tree).map(_.eval) flatMap {
        case None => Activity.exception(noBrokersAvailableException)
        case Some(set) if set.isEmpty => Activity.exception(noBrokersAvailableException)
        case Some(set) if set.size == 1 => Activity.value(set.head)
        case Some(set) => Activity.value(Name.all(set))
      }

      new DynNameFactory(name, nameCache.apply)
    }

    new ServiceFactoryCache[Dtab, Req, Rep](
      newFactory, statsReceiver.scope("dtabcache"),
      maxNamerCacheSize)
  }

  def apply(conn: ClientConnection): Future[Service[Req, Rep]] =
    dtabCache(Dtab.base ++ Dtab.local, conn)

  def close(deadline: Time) =
    Closable.sequence(dtabCache, nameCache).close(deadline)

  override def isAvailable = dtabCache.isAvailable
}

object BindingFactory {
  def showWithDtabLocal(path: Path) =
    if (Dtab.local.isEmpty) path.show
    else path.show + " [" + Dtab.local.show + "]"
}
