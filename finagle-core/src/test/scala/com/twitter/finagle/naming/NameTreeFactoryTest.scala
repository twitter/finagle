package com.twitter.finagle.naming

import com.twitter.finagle._
import com.twitter.finagle.factory.ServiceFactoryCache
import com.twitter.finagle.util.Rng
import com.twitter.util.{Future, Time, Timer}
import org.scalatest.FunSuite
import scala.collection.mutable

class NameTreeFactoryTest extends FunSuite {
  test("distributes requests according to weight") {
    val tree =
      NameTree.Union(
        NameTree.Weighted(
          1D,
          NameTree.Union(
            NameTree.Weighted(1D, NameTree.Leaf("foo")),
            NameTree.Weighted(1D, NameTree.Leaf("bar"))
          )
        ),
        NameTree.Weighted(1D, NameTree.Leaf("baz"))
      )

    val counts = mutable.HashMap[String, Int]()

    val factoryCache = new ServiceFactoryCache[String, Unit, Unit](
      key =>
        new ServiceFactory[Unit, Unit] {
          def apply(conn: ClientConnection): Future[Service[Unit, Unit]] = {
            val count = counts.getOrElse(key, 0)
            counts.put(key, count + 1)
            Future.value(null)
          }
          def close(deadline: Time) = Future.Done
      },
      Timer.Nil
    )

    // not the world's greatest test since it depends on the
    // implementation of Drv
    val rng = {
      val ints = Array(0, 0, 0, 1, 1)
      var intIdx = 0

      new Rng {
        def nextDouble() =
          throw new UnsupportedOperationException

        def nextInt(n: Int) = {
          val i = ints(intIdx)
          intIdx += 1
          i
        }

        def nextInt() = ???
        def nextLong(n: Long) = ???
      }
    }

    val factory = NameTreeFactory(Path.empty, tree, factoryCache, rng)

    factory.apply(ClientConnection.nil)
    factory.apply(ClientConnection.nil)
    factory.apply(ClientConnection.nil)

    assert(counts("foo") == 1)
    assert(counts("bar") == 1)
    assert(counts("baz") == 1)
  }

  test("is available iff all leaves are available") {
    def isAvailable(tree: NameTree[Status]): Boolean =
      NameTreeFactory(
        Path.empty,
        tree,
        new ServiceFactoryCache[Status, Unit, Unit](
          key =>
            new ServiceFactory[Unit, Unit] {
              def apply(conn: ClientConnection): Future[Service[Unit, Unit]] = Future.value(null)
              def close(deadline: Time) = Future.Done
              override def status = key
          },
          Timer.Nil
        )
      ).isAvailable

    assert(
      isAvailable(
        NameTree.Union(
          NameTree.Weighted(
            1D,
            NameTree.Union(
              NameTree.Weighted(1D, NameTree.Leaf(Status.Open)),
              NameTree.Weighted(1D, NameTree.Leaf(Status.Open))
            )
          ),
          NameTree.Weighted(1D, NameTree.Leaf(Status.Open))
        )
      )
    )

    assert(
      !isAvailable(
        NameTree.Union(
          NameTree.Weighted(
            1D,
            NameTree.Union(
              NameTree.Weighted(1D, NameTree.Leaf(Status.Open)),
              NameTree.Weighted(1D, NameTree.Leaf(Status.Closed))
            )
          ),
          NameTree.Weighted(1D, NameTree.Leaf(Status.Open))
        )
      )
    )

    assert(
      !isAvailable(
        NameTree.Union(
          NameTree.Weighted(
            1D,
            NameTree.Union(
              NameTree.Weighted(1D, NameTree.Leaf(Status.Open)),
              NameTree.Weighted(1D, NameTree.Leaf(Status.Open))
            )
          ),
          NameTree.Weighted(1D, NameTree.Empty)
        )
      )
    )
  }
}
