package com.twitter.finagle.naming

import com.twitter.app.GlobalFlag
import com.twitter.finagle._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.factory.ServiceFactoryCache
import com.twitter.finagle.util.Drv
import com.twitter.finagle.util.Rng
import com.twitter.util.Future
import com.twitter.util.Time

object retainUneachableUnionBranches
    extends GlobalFlag[Boolean](
      true,
      "Retain empty and unresolved branches from nametree unions. " +
        "Disabling this may help to avoid certain failures associated with " +
        "name resolution issues."
    )

/**
 * A finagle context key that can be used to attach a custom id to a request
 * This id will be used to ensure that all requests with the same id are directed to the
 * same branch of a weighted [[NameTree.Union]]
 *
 *  @note Using this custom routing may violate the expected overall distribution of your requests.
 *       Use with caution.
 */
private[twitter] object CustomNameTreeFactoryKey extends Contexts.local.Key[Long]

/**
 * Builds a factory from a [[com.twitter.finagle.NameTree]]. Leaves
 * are taken from the given
 * [[com.twitter.finagle.factory.ServiceFactoryCache]]; Unions become
 * random weighted distributors.
 */
private object NameTreeFactory {

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

    case class Weighted(drv: Drv, factories: Seq[ServiceFactory[Req, Rep]])
        extends ServiceFactory[Req, Rep] {
      def apply(conn: ClientConnection) = {
        Contexts.local.get(CustomNameTreeFactoryKey) match {
          case None => factories(drv(rng)).apply(conn)
          case Some(l: Long) => factories(drv(Rng(l))).apply(conn)
        }
      }

      override def status = Status.worstOf[ServiceFactory[Req, Rep]](factories, _.status)
      def close(deadline: Time) = Future.Done
    }

    // We want to keep a sub tree in a union if it is reachable
    // or if it's explicitly weighted more than 0 and we are configured
    // to retain unreachable subtrees (default True)
    def shouldKeepInUnion(t: NameTree.Weighted[Key]): Boolean = {
      t.tree match {
        case NameTree.Neg | NameTree.Fail | NameTree.Empty =>
          retainUneachableUnionBranches() && t.weight > 0.0
        case _ => true
      }
    }

    def factoryOfTree(tree: NameTree[Key]): ServiceFactory[Req, Rep] =
      tree match {
        case NameTree.Neg | NameTree.Fail | NameTree.Empty => noBrokersAvailableFactory
        case NameTree.Leaf(key) => Leaf(key)

        // it's an invariant of Namer.bind that it returns no Alts
        case NameTree.Alt(_*) => Failed(new IllegalArgumentException("NameTreeFactory"))

        case NameTree.Union(weightedTrees @ _*) =>
          val (weights, trees) = weightedTrees
            .filter(shouldKeepInUnion)
            .unzip { case NameTree.Weighted(w, t) => (w, t) }

          if (weights.isEmpty) {
            noBrokersAvailableFactory
          } else {
            Weighted(Drv.fromWeights(weights), trees.map(factoryOfTree))
          }
      }

    factoryOfTree(tree)
  }
}
