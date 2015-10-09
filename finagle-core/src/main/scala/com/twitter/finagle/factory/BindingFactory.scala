package com.twitter.finagle.factory

import com.twitter.finagle._
import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.naming.NameInterpreter
import com.twitter.finagle.param.{Label, Stats}
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.util.{Drv, Rng, Showable}
import com.twitter.util._
import scala.collection.immutable

/**
 * Proxies requests to the current definiton of 'name', queueing
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
    q: immutable.Queue[(ClientConnection, Promise[Service[Req, Rep]], Stopwatch.Elapsed)]
  ) extends State
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
    case Activity.Ok(name) => synchronized {
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

    case Activity.Failed(exc) => synchronized {
      state match {
        case Pending(q) =>
          for ((_, p, elapsed) <- q) {
            latencyStat.add(elapsed().inMicroseconds)
            p.setException(Failure.adapt(exc, Failure.Naming))
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
        Trace.record("namer.success")
        cache(name, conn)

      case Failed(exc) =>
        Trace.recordBinary("namer.failure", exc.getClass.getName)
        Future.exception(Failure.adapt(exc, Failure.Naming))

      case Closed() =>
        Trace.record("namer.closed")
        // don't trace these, since they're not a namer failure
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
        p setInterruptHandler { case exc =>
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
        for ((_, p, elapsed) <- q) {
          latencyStat.add(elapsed().inMicroseconds)
          p.setException(exc)
        }
      case _ =>
    }
    sub.close(deadline)
  }
}

/**
 * Builds a factory from a [[com.twitter.finagle.NameTree]]. Leaves
 * are taken from the given
 * [[com.twitter.finagle.factory.ServiceFactoryCache]]; Unions become
 * random weighted distributors.
 */
private[finagle] object NameTreeFactory {

  def apply[Key, Req, Rep](
    path: Path,
    tree: NameTree[Key],
    factoryCache: ServiceFactoryCache[Key, Req, Rep],
    rng: Rng = Rng.threadLocal
  ): ServiceFactory[Req, Rep] = {

    lazy val noBrokersAvailableFactory = Failed(new NoBrokersAvailableException(path.show))

    case class Failed(exn: Throwable) extends ServiceFactory[Req, Rep] {
      val service: Future[Service[Req, Rep]] = Future.exception(exn)

      def apply(conn: ClientConnection) = service
      override def status = Status.Closed
      def close(deadline: Time) = Future.Done
    }

    case class Leaf(key: Key) extends ServiceFactory[Req, Rep] {
      def apply(conn: ClientConnection) = factoryCache.apply(key, conn)
      override def status = factoryCache.status(key)
      def close(deadline: Time) = Future.Done
    }

    case class Weighted(
      drv: Drv,
      factories: Seq[ServiceFactory[Req, Rep]]
    ) extends ServiceFactory[Req, Rep] {
      def apply(conn: ClientConnection) = factories(drv(rng)).apply(conn)

      override def status = Status.worstOf[ServiceFactory[Req, Rep]](factories, _.status)
      def close(deadline: Time) = Future.Done
    }

    def factoryOfTree(tree: NameTree[Key]): ServiceFactory[Req, Rep] =
      tree match {
        case NameTree.Neg | NameTree.Fail | NameTree.Empty => noBrokersAvailableFactory
        case NameTree.Leaf(key) => Leaf(key)

        // it's an invariant of Namer.bind that it returns no Alts
        case NameTree.Alt(_*) => Failed(new IllegalArgumentException("NameTreeFactory"))

        case NameTree.Union(weightedTrees@_*) =>
          val (weights, trees) = weightedTrees.unzip { case NameTree.Weighted(w, t) => (w, t) }
          Weighted(Drv.fromWeights(weights), trees.map(factoryOfTree))
      }

    factoryOfTree(tree)
  }
}

/**
 * A factory that routes to the local binding of the passed-in
 * [[com.twitter.finagle.Path Path]]. It calls `newFactory` to mint a
 * new [[com.twitter.finagle.ServiceFactory ServiceFactory]] for novel
 * name evaluations.
 *
 * A three-level caching scheme is employed for efficiency:
 *
 * First, the [[ServiceFactory]] for a [[Path]] is cached by the local
 * [[com.twitter.finagle.Dtab Dtab]]. This permits sharing in the
 * common case that no local [[Dtab]] is given. (It also papers over the
 * mutability of [[Dtab.base]].)
 *
 * Second, the [[ServiceFactory]] for a [[Path]] (relative to a
 * [[Dtab]]) is cached by the [[com.twitter.finagle.NameTree
 * NameTree]] it is bound to by that [[Dtab]]. Binding a path results
 * in an [[com.twitter.util.Activity Activity]], so this cache permits
 * sharing when the same tree is returned in different updates of the
 * [[Activity]]. (In particular it papers over nuisance updates of the
 * [[Activity]] where the value is unchanged.)
 *
 * Third, the ServiceFactory for a [[com.twitter.finagle.Name.Bound
 * Name.Bound]] appearing in a [[NameTree]] is cached by its
 * [[Name.Bound]]. This permits sharing when the same [[Name.Bound]]
 * appears in different [[NameTree]]s (or the same [[NameTree]]
 * resulting from different bindings of the [[Path]]).
 *
 * @bug This is far too complicated, though it seems necessary for
 * efficiency when namers are occasionally overriden.
 *
 * @bug 'status' has a funny definition.
 */
private[finagle] class BindingFactory[Req, Rep](
    path: Path,
    newFactory: Name.Bound => ServiceFactory[Req, Rep],
    baseDtab: () => Dtab = BindingFactory.DefaultBaseDtab,
    statsReceiver: StatsReceiver = NullStatsReceiver,
    maxNameCacheSize: Int = 8,
    maxNameTreeCacheSize: Int = 8,
    maxNamerCacheSize: Int = 4)
  extends ServiceFactory[Req, Rep] {

  private[this] val tree = NameTree.Leaf(path)

  private[this] val nameCache =
    new ServiceFactoryCache[Name.Bound, Req, Rep](
      bound => new ServiceFactoryProxy(newFactory(bound)) {
        private val boundShow = Showable.show(bound)
        override def apply(conn: ClientConnection) = {
          Trace.recordBinary("namer.name", boundShow)
          super.apply(conn)
        }
      },
      statsReceiver.scope("namecache"),
      maxNameCacheSize)

  private[this] val nameTreeCache =
    new ServiceFactoryCache[NameTree[Name.Bound], Req, Rep](
      tree => new ServiceFactoryProxy(NameTreeFactory(path, tree, nameCache)) {
        private val treeShow = tree.show
        override def apply(conn: ClientConnection) = {
          Trace.recordBinary("namer.tree", treeShow)
          super.apply(conn)
        }
      },
      statsReceiver.scope("nametreecache"),
      maxNameTreeCacheSize)

  private[this] val dtabCache = {
    val newFactory: ((Dtab, Dtab)) => ServiceFactory[Req, Rep] = { case (baseDtab, localDtab) =>
      val factory = new DynNameFactory(
        NameInterpreter.bind(baseDtab ++ localDtab, path),
        nameTreeCache,
        statsReceiver = statsReceiver)

      new ServiceFactoryProxy(factory) {
        private val pathShow = path.show
        private val baseDtabShow = baseDtab.show
        override def apply(conn: ClientConnection) = {
          Trace.recordBinary("namer.path", pathShow)
          Trace.recordBinary("namer.dtab.base", baseDtabShow)
          // dtab.local is annotated on the client & server tracers.

          super.apply(conn) rescue {
            // we don't have the dtabs handy at the point we throw
            // the exception; fill them in on the way out
            case e: NoBrokersAvailableException =>
              Future.exception(new NoBrokersAvailableException(e.name, baseDtab, localDtab))
          }
        }
      }
    }

    new ServiceFactoryCache[(Dtab, Dtab), Req, Rep](
      newFactory,
      statsReceiver.scope("dtabcache"),
      maxNamerCacheSize)
  }

  def apply(conn: ClientConnection): Future[Service[Req, Rep]] =
    dtabCache((baseDtab(), Dtab.local), conn)

  def close(deadline: Time) =
    Closable.sequence(dtabCache, nameTreeCache, nameCache).close(deadline)

  override def status = dtabCache.status((baseDtab(), Dtab.local))
}

object BindingFactory {
  val role = Stack.Role("Binding")

  /**
   * A class eligible for configuring a
   * [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.factory.BindingFactory]] with a destination
   * [[com.twitter.finagle.Name]] to bind.
   */
  case class Dest(dest: Name) {
    def mk(): (Dest, Stack.Param[Dest]) =
      (this, Dest.param)
  }
  object Dest {
    implicit val param = Stack.Param(Dest(Name.Path(Path.read("/$/fail"))))
  }

  private[finagle] val DefaultBaseDtab = () => Dtab.base

  /**
   * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.factory.BindingFactory]] with a
   * [[com.twitter.finagle.Dtab]].
   */
  case class BaseDtab(baseDtab: () => Dtab) {
    def mk(): (BaseDtab, Stack.Param[BaseDtab]) =
      (this, BaseDtab.param)
  }
  object BaseDtab {
    implicit val param = Stack.Param(BaseDtab(DefaultBaseDtab))
  }

  /**
   * Base type for BindingFactory modules. Implementers may handle
   * bound residual paths in a protocol-specific way.
   *
   * The module creates a new `ServiceFactory` based on the module
   * above it for each distinct [[com.twitter.finagle.Name.Bound]]
   * resolved from `BindingFactory.Dest` (with caching of previously
   * seen `Name.Bound`s).
   */
  private[finagle] trait Module[Req, Rep] extends Stack.Module[ServiceFactory[Req, Rep]] {
    val role = BindingFactory.role
    val description = "Bind destination names to endpoints"
    val parameters = Seq(
      implicitly[Stack.Param[BaseDtab]],
      implicitly[Stack.Param[Dest]],
      implicitly[Stack.Param[Label]],
      implicitly[Stack.Param[Stats]])

    /**
     * A request filter that is aware of the bound residual path.
     *
     * The returned filter is applied around the ServiceFactory built from the rest of the stack.
     */
    protected[this] def boundPathFilter(path: Path): Filter[Req, Rep, Req, Rep]

    def make(params: Stack.Params, next: Stack[ServiceFactory[Req, Rep]]) = {
      val Label(label) = params[Label]
      val Stats(stats) = params[Stats]
      val Dest(dest) = params[Dest]

      def newStack(errorLabel: String, bound: Name.Bound) = {
        val client = next.make(
          params +
          // replace the possibly unbound Dest with the definitely bound
          // Dest because (1) it's needed by AddrMetadataExtraction and
          // (2) it seems disingenuous not to.
          Dest(bound) +
          LoadBalancerFactory.Dest(bound.addr) +
          LoadBalancerFactory.ErrorLabel(errorLabel))

        boundPathFilter(bound.path) andThen client
      }

      val factory = dest match {
        case bound@Name.Bound(addr) => newStack(label, bound)

        case Name.Path(path) =>
          val BaseDtab(baseDtab) = params[BaseDtab]
          new BindingFactory(path, newStack(path.show, _), baseDtab, stats.scope("namer"))
      }

      Stack.Leaf(role, factory)
    }
  }

  /**
   * Creates a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.factory.BindingFactory]].
   *
   * Ignores bound residual paths.
   */
  private[finagle] def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Module[Req, Rep] {
      private[this] val f = Filter.identity[Req, Rep]
      protected[this] def boundPathFilter(path: Path) = f
    }
}
